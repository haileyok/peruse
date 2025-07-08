package peruse

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/labstack/echo/v4"
)

type SeattleFeed struct {
	conn           driver.Conn
	logger         *slog.Logger
	cached         []RankedFeedPost
	cacheExpiresAt time.Time
	mu             sync.RWMutex
}

func NewSeattleFeed(s *Server) *SeattleFeed {
	logger := s.logger.With("feed", "seattle-feed")
	return &SeattleFeed{
		conn:   s.conn,
		logger: logger,
	}
}

func (f *SeattleFeed) Name() string {
	return "seattle"
}

func (f *SeattleFeed) HandleGetFeedSkeleton(e echo.Context, req FeedSkeletonRequest) error {
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
