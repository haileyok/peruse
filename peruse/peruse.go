package peruse

import (
	"context"
	"crypto"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/haileyok/peruse/wikidata"
	"github.com/haileyok/photocopy/nervana"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	slogecho "github.com/samber/slog-echo"
	"golang.org/x/time/rate"
)

type Server struct {
	httpd         *http.Server
	echo          *echo.Echo
	conn          driver.Conn
	logger        *slog.Logger
	args          *ServerArgs
	keyCache      *lru.Cache[string, crypto.PublicKey]
	directory     identity.Directory
	userManager   *UserManager
	xrpc          *xrpc.Client
	feeds         map[string]Feed
	cursor        string
	nervanaClient *nervana.Client
}

type ServerArgs struct {
	Logger               *slog.Logger
	HttpAddr             string
	ClickhouseAddr       string
	ClickhouseDatabase   string
	ClickhouseUser       string
	ClickhousePass       string
	FeedOwnerDid         string
	ServiceDid           string
	ServiceEndpoint      string
	ChronoFeedRkey       string
	SuggestedFollowsRkey string
	CursorFile           string
	RelayHost            string
	NervanaEndpoint      string
	NervanaApiKey        string
}

type Feed interface {
	Name() string
	FeedSkeleton(e echo.Context, req FeedSkeletonRequest) error
	OnPost(ctx context.Context, post *bsky.FeedPost, uri, did, rkey, cid string, indexedAt time.Time, nerItems []nervana.NervanaItem) error
	OnLike(ctx context.Context, like *bsky.FeedLike, uri, did, rkey, cid string, indexedAt time.Time) error
	OnRepost(ctx context.Context, repost *bsky.FeedRepost, uri, did, rkey, cid string, indexedAt time.Time) error
}

func NewServer(args ServerArgs) (*Server, error) {
	if args.Logger == nil {
		args.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	e := echo.New()
	e.Use(middleware.RemoveTrailingSlash())
	e.Use(slogecho.New(args.Logger))
	e.Use(middleware.Recover())

	httpd := &http.Server{
		Addr:    args.HttpAddr,
		Handler: e,
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{args.ClickhouseAddr},
		Auth: clickhouse.Auth{
			Database: args.ClickhouseDatabase,
			Username: args.ClickhouseUser,
			Password: args.ClickhousePass,
		},
	})
	if err != nil {
		return nil, err
	}

	kc, _ := lru.New[string, crypto.PublicKey](100_000)

	baseDir := identity.BaseDirectory{
		PLCURL: "https://plc.directory",
		HTTPClient: http.Client{
			Timeout: time.Second * 5,
		},
		PLCLimiter:            rate.NewLimiter(rate.Limit(10), 1), // TODO: what is this rate limit anyway?
		TryAuthoritativeDNS:   false,
		SkipDNSDomainSuffixes: []string{".bsky.social", ".staging.bsky.dev"},
	}

	dir := identity.NewCacheDirectory(&baseDir, 100_000, time.Hour*48, time.Minute*15, time.Minute*15)

	nervanaClient := nervana.NewClient(args.NervanaEndpoint, args.NervanaApiKey)

	return &Server{
		echo:        e,
		httpd:       httpd,
		conn:        conn,
		args:        &args,
		logger:      args.Logger,
		keyCache:    kc,
		directory:   &dir,
		userManager: NewUserManager(),
		xrpc: &xrpc.Client{
			Host: "https://public.api.bsky.app",
		},
		feeds:         map[string]Feed{},
		nervanaClient: nervanaClient,
	}, nil
}

func (s *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.addFeed(NewWikidataFeed(ctx, s, "seattle", "seattle_post", wikidata.SeattleEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "los-angeles", "los_angeles_post", wikidata.LosAngelesEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "san-francisco", "san_francisco_post", wikidata.SanFranciscoEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "austin", "austin_post", wikidata.AustinEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "chicago", "chicago_post", wikidata.ChicagoEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "boston", "boston_post", wikidata.BostonEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "software", "software_post", wikidata.SoftwareEntities))
	s.addFeed(NewWikidataFeed(ctx, s, "baseball", "baseball_post", wikidata.BaseballEntities))

	s.addRoutes()

	go func() {
		if err := s.httpd.ListenAndServe(); err != nil {
			s.logger.Error("error starting http server", "error", err)
		}
	}()

	go func(ctx context.Context, cancel context.CancelFunc) {
		if err := s.startConsumer(ctx, cancel); err != nil {
			s.logger.Error("error starting consumer", "error", err)
		}
	}(ctx, cancel)

	<-ctx.Done()

	s.logger.Info("shutting down server...")

	s.conn.Close()

	return nil
}

func (s *Server) addFeed(f Feed) error {
	_, exists := s.feeds[f.Name()]
	if exists {
		return fmt.Errorf("feed %s already exists", f.Name())
	}
	s.feeds[f.Name()] = f
	return nil
}

func (s *Server) addRoutes() {
	s.echo.GET("/xrpc/app.bsky.feed.getFeedSkeleton", s.handleFeedSkeleton, s.handleAuthMiddleware)
	s.echo.GET("/xrpc/app.bsky.feed.describeFeedGenerator", s.handleDescribeFeedGenerator)
	s.echo.GET("/.well-known/did.json", s.handleWellKnown)
	s.echo.GET("/api/getSuggestedFollows", s.handleGetSuggestedFollows)
}

func (s *Server) handleAuthMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(e echo.Context) error {
		auth := e.Request().Header.Get("authorization")
		pts := strings.Split(auth, " ")
		if auth == "" || len(pts) != 2 || pts[0] != "Bearer" {
			return helpers.InputError(e, "AuthRequired", "")
		}

		did, err := s.checkJwt(e.Request().Context(), pts[1])
		if err != nil {
			return helpers.InputError(e, "AuthRequired", err.Error())
		}

		u := s.userManager.getUser(did)

		e.Set("user", u)

		return next(e)
	}
}

func urisToFeedPostItems(uris []string) []FeedPostItem {
	var pis []FeedPostItem
	for _, u := range uris {
		pis = append(pis, FeedPostItem{
			Post: u,
		})
	}
	return pis
}
