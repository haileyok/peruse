package peruse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
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

type WikidataFeed struct {
	conn           driver.Conn
	logger         *slog.Logger
	cached         []RankedFeedPost
	cacheExpiresAt time.Time
	mu             sync.RWMutex
	nervanaClient  *nervana.Client
	entities       map[string]wikidata.Entity
	inserter       *clickhouse_inserter.Inserter
	feedName       string
	tableName      string
}

type RankedFeedPost struct {
	LikeCt     uint64    `ch:"like_ct"`
	Uri        string    `ch:"uri"`
	CreatedAt  time.Time `ch:"created_at"`
	HoursOld   int64     `ch:"hours_old"`
	DecayScore float64   `ch:"decay_score"`
}

func NewWikidataFeed(ctx context.Context, s *Server, feedName string, tableName string, entitiesJson string) *WikidataFeed {
	logger := s.logger.With("feed", feedName)

	// TODO: just make this the `Unmarshal` of the `wikidata.Entity` struct
	var entitiesArr []wikidata.Entity
	if err := json.Unmarshal([]byte(entitiesJson), &entitiesArr); err != nil {
		panic(err)
	}

	entities := map[string]wikidata.Entity{}
	for _, e := range entitiesArr {
		entityPts := strings.Split(e.Entity, "/")
		if len(entityPts) == 0 {
			continue
		}
		entityId := entityPts[len(entityPts)-1]

		propertyPts := strings.Split(e.Property, "/")
		if len(propertyPts) == 0 {
			continue
		}
		propertyId := propertyPts[len(propertyPts)-1]

		instanceOfPts := strings.Split(e.InstanceOf, "/")
		if len(instanceOfPts) == 0 {
			continue
		}
		instanceOfId := instanceOfPts[len(instanceOfPts)-1]

		entities[entityId] = wikidata.Entity{
			Entity:     entityId,
			Property:   propertyId,
			InstanceOf: instanceOfId,
		}
	}

	inserter, err := clickhouse_inserter.New(ctx, &clickhouse_inserter.Args{
		PrometheusCounterPrefix: "peruse_wikidata_" + feedName,
		BatchSize:               1,
		Logger:                  s.logger,
		Conn:                    s.conn,
		Query:                   fmt.Sprintf("INSERT INTO %s (uri, created_at)", tableName),
		RateLimit:               3,
	})
	if err != nil {
		panic(err)
	}

	return &WikidataFeed{
		conn:          s.conn,
		logger:        logger,
		nervanaClient: s.nervanaClient,
		entities:      entities,
		inserter:      inserter,
		feedName:      feedName,
		tableName:     tableName,
	}
}

func (f *WikidataFeed) Name() string {
	return f.feedName
}

func (f *WikidataFeed) FeedSkeleton(e echo.Context, req FeedSkeletonRequest) error {
	ctx := e.Request().Context()

	var cursor int
	if req.Cursor != "" {
		cursor64, err := strconv.ParseInt(req.Cursor, 10, 32)
		if err != nil {
			f.logger.Error("error converting cursor", "error", err)
		}
		cursor = int(cursor64)
	}

	posts, err := f.getPosts(ctx)
	if err != nil {
		f.logger.Error("error getting posts", "error", err)
		return helpers.ServerError(e, "FeedError", "Unable to get posts for feed")
	}

	if len(posts) < cursor {
		cursor = len(posts)
	}

	posts = posts[cursor:]

	if len(posts) > 30 {
		posts = posts[:30]
	}

	var items []FeedPostItem
	for _, p := range posts {
		items = append(items, FeedPostItem{
			Post: p.Uri,
		})
	}

	newCursor := fmt.Sprintf("%d", cursor+len(posts))

	return e.JSON(200, FeedSkeletonResponse{
		Feed:   items,
		Cursor: &newCursor,
	})
}

type FeedDatabaseItem struct {
	Uri       string    `ch:"uri"`
	CreatedAt time.Time `ch:"created_at"`
}

func (f *WikidataFeed) OnPost(ctx context.Context, post *bsky.FeedPost, uri, did, rkey, cid string, indexedAt time.Time, nerItems []nervana.NervanaItem) error {
	if post.Reply != nil {
		return nil
	}

	if post.Text == "" {
		return nil
	}

	if wikidata.ShouldInclude(ctx, f.entities, nerItems) {
		fdi := FeedDatabaseItem{
			Uri:       uri,
			CreatedAt: indexedAt,
		}
		if err := f.inserter.Insert(ctx, fdi); err != nil {
			return err
		}
	}

	return nil
}

func (f *WikidataFeed) OnLike(ctx context.Context, like *bsky.FeedLike, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *WikidataFeed) OnRepost(ctx context.Context, repost *bsky.FeedRepost, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *WikidataFeed) getPosts(ctx context.Context) ([]RankedFeedPost, error) {
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

	if err := f.conn.Select(ctx, &posts, makeCityQuery(f.tableName)); err != nil {
		return nil, err
	}
	f.cached = posts
	f.cacheExpiresAt = now.Add(1 * time.Minute)

	return posts, nil
}

func makeCityQuery(tableName string) string {
	return fmt.Sprintf(`
SELECT 
    count(*) as like_ct,
    sp.uri,
    sp.created_at,
    dateDiff('hour', sp.created_at, now()) as hours_old,
    count(*) * exp(-0.1 * dateDiff('hour', sp.created_at, now())) as decay_score
FROM %s sp 
LEFT JOIN default.like_by_subject i ON sp.uri = i.subject_uri 
WHERE sp.created_at > now() - INTERVAL 1 DAY 
GROUP BY sp.uri, sp.created_at 
ORDER BY decay_score DESC
LIMIT 5000
		`, tableName)
}
