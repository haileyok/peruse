package peruse

import (
	"github.com/labstack/echo/v4"
)

type describeFeedGeneratorResponse struct {
	Did   string   `json:"did"`
	Feeds []string `json:"feeds"`
}

func makeFeedUri(accountDid, rkey string) string {
	return "at://" + accountDid + "/app.bsky.feed.generator/" + rkey
}

func (s *Server) handleDescribeFeedGenerator(e echo.Context) error {
	feedUris := []string{
		makeFeedUri(s.args.FeedOwnerDid, s.args.ChronoFeedRkey),
	}

	return e.JSON(200, &describeFeedGeneratorResponse{
		Did:   s.args.ServiceDid,
		Feeds: feedUris,
	})
}
