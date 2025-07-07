package peruse

import (
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/haileyok/peruse/internal/helpers"
	"github.com/labstack/echo/v4"
)

type FeedSkeletonRequest struct {
	Feed   string `query:"feed"`
	Cursor string `query:"cursor"`
}

type FeedSkeletonResponse struct {
	Cursor *string        `json:"cursor,omitempty"`
	Feed   []FeedPostItem `json:"feed"`
}

type FeedPostItem struct {
	Post   string  `json:"post"`
	Reason *string `json:"reason,omitempty"`
}

func (s *Server) handleFeedSkeleton(e echo.Context) error {
	var req FeedSkeletonRequest
	if err := e.Bind(&req); err != nil {
		s.logger.Error("unable to bind feed skeleton request", "error", err)
		return helpers.ServerError(e, "", "")
	}

	aturi, err := syntax.ParseATURI(req.Feed)
	if err != nil {
		return helpers.InputError(e, "InvalidFeed", "")
	}

	feed, exists := s.feeds[aturi.RecordKey().String()]
	if !exists {
		// TODO: refactor these feeds to work with addFeed
		switch aturi.RecordKey().String() {
		case s.args.ChronoFeedRkey:
			return s.handleChronoFeed(e, req)
		case s.args.SuggestedFollowsRkey:
			return s.handleSuggestedFollowsFeed(e, req)
		default:
			s.logger.Warn("invalid feed requested", "requested-feed", req.Feed)
			return helpers.InputError(e, "FeedNotFound", "")
		}
	}

	return feed.HandleGetFeedSkeleton(e, req)
}
