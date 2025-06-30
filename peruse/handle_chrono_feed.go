package peruse

import (
	"fmt"

	"github.com/haileyok/peruse/internal/helpers"
	"github.com/haileyok/photocopy/models"
	"github.com/labstack/echo/v4"
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
		cbdids = append(cbdids, cb.Did)
	}
	cbdids = cbdids[1:] // remove self

	if req.Cursor == "" {
		req.Cursor = "9999999999999" // hack for simplicity...
	}

	var posts []models.Post
	if err := s.conn.Select(ctx, &posts, fmt.Sprintf(`
		SELECT uri
		FROM default.post
		WHERE did IN (?)
		AND rkey < ?
		ORDER BY created_at DESC
		LIMIT 50
		`), cbdids, req.Cursor); err != nil {
		s.logger.Error("error getting close by chrono posts", "error", err)
		return helpers.ServerError(e, "FeedError", "")
	}

	if len(posts) == 0 {
		return helpers.ServerError(e, "FeedError", "Not enough posts")
	}

	var fpis []FeedPostItem

	for _, p := range posts {
		fpis = append(fpis, FeedPostItem{
			Post: p.Uri,
		})
	}

	return e.JSON(200, FeedSkeletonResponse{
		Cursor: &posts[len(posts)-1].Rkey,
		Feed:   fpis,
	})
}
