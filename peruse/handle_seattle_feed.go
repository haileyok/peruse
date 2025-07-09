package peruse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/haileyok/peruse/wikidata"
	"github.com/haileyok/photocopy/clickhouse_inserter"
	"github.com/haileyok/photocopy/nervana"
	"github.com/labstack/echo/v4"
)

type SeattleFeed struct {
	conn           driver.Conn
	logger         *slog.Logger
	cached         []RankedFeedPost
	cacheExpiresAt time.Time
	mu             sync.RWMutex
	nervanaClient  *nervana.Client
	entityIds      map[string]bool
	inserter       *clickhouse_inserter.Inserter
}

func NewSeattleFeed(ctx context.Context, s *Server) *SeattleFeed {
	logger := s.logger.With("feed", "seattle-feed")

	var entities []wikidata.Entity
	if err := json.Unmarshal([]byte(wikidata.SeattleEntities), &entities); err != nil {
		panic(err)
	}

	entitiyIds := map[string]bool{}
	for _, e := range entities {
		pts := strings.Split(e.Entity, "/")
		if len(pts) == 0 {
			continue
		}
		id := pts[len(pts)-1]
		if !strings.HasPrefix(id, "Q") {
			continue
		}
		entitiyIds[id] = true
	}

	inserter, err := clickhouse_inserter.New(ctx, &clickhouse_inserter.Args{
		PrometheusCounterPrefix: "photocopy_follows",
		BatchSize:               1,
		Logger:                  s.logger,
		Conn:                    s.conn,
		Query:                   "INSERT INTO seattle_post (uri, created_at)",
		RateLimit:               3,
	})
	if err != nil {
		panic(err)
	}

	return &SeattleFeed{
		conn:          s.conn,
		logger:        logger,
		nervanaClient: s.nervanaClient,
		entityIds:     entitiyIds,
		inserter:      inserter,
	}
}

func (f *SeattleFeed) Name() string {
	return "seattle"
}

func (f *SeattleFeed) FeedSkeleton(e echo.Context, req FeedSkeletonRequest) error {
	ctx := e.Request().Context()

	cursor, err := getTimeBasedCursor(req)
	if err != nil {
		f.logger.Error("error getting cursor", "error", err)
		return helpers.InputError(e, "FeedError", "Invalid cursor for feed")
	}

	posts, err := f.getPosts(ctx)
	if err != nil {
		f.logger.Error("error getting posts", "error", err)
		return helpers.ServerError(e, "FeedError", "Unable to get posts for feed")
	}

	for i, p := range posts {
		if p.CreatedAt.Before(cursor) {
			posts = posts[i:]
			break
		}
	}

	if len(posts) > 30 {
		posts = posts[:30]
	}

	var items []FeedPostItem
	for _, p := range posts {
		items = append(items, FeedPostItem{
			Post: p.Uri,
		})
	}

	newCursor := fmt.Sprintf("%d", posts[len(posts)-1].CreatedAt.UnixMilli())

	return e.JSON(200, FeedSkeletonResponse{
		Feed:   items,
		Cursor: &newCursor,
	})
}

type FeedDatabaseItem struct {
	Uri       string    `ch:"uri"`
	CreatedAt time.Time `ch:"created_at"`
}

func (f *SeattleFeed) OnPost(ctx context.Context, post *bsky.FeedPost, uri, did, rkey, cid string, indexedAt time.Time) error {
	if post.Reply != nil {
		return nil
	}

	if post.Text == "" {
		return nil
	}

	nerItems, err := f.nervanaClient.MakeRequest(ctx, post.Text)
	if err != nil {
		return err
	}

	for _, item := range nerItems {
		if f.entityIds[item.EntityId] {
			fdi := FeedDatabaseItem{
				Uri:       uri,
				CreatedAt: indexedAt,
			}
			if err := f.inserter.Insert(ctx, fdi); err != nil {
				return err
			}
			break
		}
	}

	return nil
}

func (f *SeattleFeed) OnLike(ctx context.Context, like *bsky.FeedLike, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *SeattleFeed) OnRepost(ctx context.Context, repost *bsky.FeedRepost, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *SeattleFeed) getPosts(ctx context.Context) ([]RankedFeedPost, error) {
	now := time.Now()
	f.mu.RLock()
	expiresAt := f.cacheExpiresAt
	posts := f.cached
	f.mu.RUnlock()
	if posts != nil && now.Before(expiresAt) {
		return posts, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.cached != nil && now.Before(expiresAt) {
		return f.cached, nil
	}

	if err := f.conn.Select(ctx, &posts, seattleQuery); err != nil {
		return nil, err
	}
	f.cached = posts
	f.cacheExpiresAt = now

	return posts, nil
}

var seattleQuery = `
SELECT 
    count(*) as like_ct,
    sp.uri,
    sp.created_at,
    dateDiff('hour', sp.created_at, now()) as hours_old,
    count(*) * exp(-0.1 * dateDiff('hour', sp.created_at, now())) as decay_score
FROM seattle_post sp 
LEFT JOIN default.like_by_subject i ON sp.uri = i.subject_uri 
WHERE sp.created_at > now() - INTERVAL 1 DAY 
GROUP BY sp.uri, sp.created_at 
ORDER BY decay_score DESC
LIMIT 5000
	`
