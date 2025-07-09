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

type CityAreaFeed struct {
	conn           driver.Conn
	logger         *slog.Logger
	cached         []RankedFeedPost
	cacheExpiresAt time.Time
	mu             sync.RWMutex
	nervanaClient  *nervana.Client
	entityIds      map[string]bool
	inserter       *clickhouse_inserter.Inserter
	feedName       string
	tableName      string
}

func NewCityAreaFeed(ctx context.Context, s *Server, feedName string, tableName string, entitiesJson string) *CityAreaFeed {
	logger := s.logger.With("feed", feedName)

	var entities []wikidata.Entity
	if err := json.Unmarshal([]byte(entitiesJson), &entities); err != nil {
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
		PrometheusCounterPrefix: "peruse_city_area_" + feedName,
		BatchSize:               1,
		Logger:                  s.logger,
		Conn:                    s.conn,
		Query:                   fmt.Sprintf("INSERT INTO %s (uri, created_at)", tableName),
		RateLimit:               3,
	})
	if err != nil {
		panic(err)
	}

	return &CityAreaFeed{
		conn:          s.conn,
		logger:        logger,
		nervanaClient: s.nervanaClient,
		entityIds:     entitiyIds,
		inserter:      inserter,
		feedName:      feedName,
		tableName:     tableName,
	}
}

func (f *CityAreaFeed) Name() string {
	return f.feedName
}

func (f *CityAreaFeed) FeedSkeleton(e echo.Context, req FeedSkeletonRequest) error {
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

func (f *CityAreaFeed) OnPost(ctx context.Context, post *bsky.FeedPost, uri, did, rkey, cid string, indexedAt time.Time, nerItems []nervana.NervanaItem) error {
	if post.Reply != nil {
		return nil
	}

	if post.Text == "" {
		return nil
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

func (f *CityAreaFeed) OnLike(ctx context.Context, like *bsky.FeedLike, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *CityAreaFeed) OnRepost(ctx context.Context, repost *bsky.FeedRepost, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *CityAreaFeed) getPosts(ctx context.Context) ([]RankedFeedPost, error) {
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
	f.cacheExpiresAt = now

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
