package peruse

import (
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/labstack/echo/v4"
)

func (s *Server) handleSuggestedFollowsFeed(e echo.Context, req FeedSkeletonRequest) error {
	ctx := e.Request().Context()
	u := e.Get("user").(*User)

	suggFollows, err := u.getSuggestedFollows(ctx, s)
	if err != nil {
		s.logger.Error("error getting suggested follows for user", "user", u.did, "error", err)
		return helpers.ServerError(e, "FeedError", "")
	}

	suggDids := []string{}
	for _, sugg := range suggFollows {
		suggDids = append(suggDids, sugg.SuggestedDid)
	}

	if req.Cursor == "" {
		req.Cursor = DefaultCursor
	}

	posts, err := s.getPostsForDidsChronological(ctx, suggDids, req.Cursor)
	if err != nil {
		s.logger.Error("error getting suggested follows chrono posts", "error", err)
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
