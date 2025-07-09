package peruse

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/haileyok/photocopy/nervana"
	"github.com/labstack/echo/v4"
)

type BaseballFeed struct {
	conn           driver.Conn
	logger         *slog.Logger
	cached         []RankedFeedPost
	cacheExpiresAt time.Time
	mu             sync.RWMutex
}

type RankedFeedPost struct {
	LikeCt     uint64    `ch:"like_ct"`
	Uri        string    `ch:"uri"`
	CreatedAt  time.Time `ch:"created_at"`
	HoursOld   int64     `ch:"hours_old"`
	DecayScore float64   `ch:"decay_score"`
}

func NewBaseballFeed(s *Server) *BaseballFeed {
	logger := s.logger.With("feed", "baseball-feed")
	return &BaseballFeed{
		conn:   s.conn,
		logger: logger,
		cached: nil,
	}
}

func (f *BaseballFeed) Name() string {
	return "baseball"
}

func (f *BaseballFeed) FeedSkeleton(e echo.Context, req FeedSkeletonRequest) error {
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

	if len(posts) == 0 {
		return helpers.ServerError(e, "FeedError", "Not enough posts")
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
		items = append(items, FeedPostItem{Post: p.Uri})
	}

	newCursor := fmt.Sprintf("%d", posts[len(posts)-1].CreatedAt.UnixMilli())

	return e.JSON(200, FeedSkeletonResponse{
		Feed:   items,
		Cursor: &newCursor,
	})
}

func (f *BaseballFeed) OnPost(ctx context.Context, post *bsky.FeedPost, uri, did, rkey, cid string, indexedAt time.Time, nerItems []nervana.NervanaItem) error {
	return nil
}

func (f *BaseballFeed) OnLike(ctx context.Context, like *bsky.FeedLike, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *BaseballFeed) OnRepost(ctx context.Context, repost *bsky.FeedRepost, uri, did, rkey, cid string, indexedAt time.Time) error {
	return nil
}

func (f *BaseballFeed) getPosts(ctx context.Context) ([]RankedFeedPost, error) {
	f.mu.RLock()
	f.mu.RUnlock()
	if f.cached != nil && time.Now().Before(f.cacheExpiresAt) {
		return f.cached, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.cached != nil && time.Now().Before(f.cacheExpiresAt) {
		return f.cached, nil
	}

	var posts []RankedFeedPost
	if err := f.conn.Select(ctx, &posts, baseballQuery); err != nil {
		return nil, err
	}
	f.cached = posts
	f.cacheExpiresAt = time.Now().Add(1 * time.Minute)
	return posts, nil
}

func msToTime(ms string) (time.Time, error) {
	msInt, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, msInt*int64(time.Millisecond)), nil
}

var baseballQuery = `
SELECT 
    count(*) as like_ct,
    bp.uri,
    bp.created_at,
    dateDiff('hour', bp.created_at, now()) as hours_old,
    count(*) * exp(-0.1 * dateDiff('hour', bp.created_at, now())) as decay_score
FROM baseball_post bp 
LEFT JOIN default.like_by_subject i ON bp.uri = i.subject_uri 
WHERE bp.created_at > now() - INTERVAL 1 DAY 
GROUP BY bp.uri, bp.created_at 
ORDER BY decay_score DESC
LIMIT 5000
`

func getTimeBasedCursor(req FeedSkeletonRequest) (time.Time, error) {
	cursor := time.Now()
	if req.Cursor != "" {
		maybeCursor, err := msToTime(req.Cursor)
		if err != nil {
			return time.Time{}, fmt.Errorf("error getting time from cursor: %w", err)
		}
		cursor = maybeCursor
	}
	return cursor, nil
}
