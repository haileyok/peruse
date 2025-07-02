package peruse

import (
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/labstack/echo/v4"
)

const (
	DefaultCursor = "9999999999999"
)

func (s *Server) handleChronoFeed(e echo.Context, req FeedSkeletonRequest) error {
	ctx := e.Request().Context()
	u := e.Get("user").(*User)

	closeBy, err := u.getCloseBy(ctx, s)
	if err != nil {
		s.logger.Error("error getting close by for user", "user", u.did, "error", err)
		return helpers.ServerError(e, "FeedError", "")
	}

	cbdids := []string{}
	for _, cb := range closeBy {
		cbdids = append(cbdids, cb.SuggestedDid)
	}
	cbdids = cbdids[1:] // remove self

	if req.Cursor == "" {
		req.Cursor = DefaultCursor // hack for simplicity...
	}

	posts, err := s.getPostsForDidsChronological(ctx, cbdids, req.Cursor)
	if err != nil {
		s.logger.Error("error getting close by chrono posts", "error", err)
		return helpers.ServerError(e, "FeedError", "")
	}

	if len(posts) == 0 {
		return helpers.ServerError(e, "FeedError", "Not enough posts")
	}

	fpis, cursor := modelPostsToFeedItems(posts)

	return e.JSON(200, FeedSkeletonResponse{
		Cursor: &cursor,
		Feed:   fpis,
	})
}
