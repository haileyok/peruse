package peruse

import (
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/labstack/echo/v4"
)

type GetSuggestedFollowsRequest struct {
	Handle      string `query:"handle"`
	ShowHandles bool   `query:"showHandles"`
}

func (s *Server) handleGetSuggestedFollows(e echo.Context) error {
	ctx := e.Request().Context()

	var req GetSuggestedFollowsRequest
	if err := e.Bind(&req); err != nil {
		return e.String(500, err.Error())
	}

	if req.Handle == "" {
		return e.String(400, "no input handle provided")
	}

	if !strings.HasPrefix(req.Handle, "did:plc:") {
		resp, err := atproto.IdentityResolveHandle(ctx, s.xrpc, req.Handle)
		if err != nil {
			return e.String(400, fmt.Sprintf("error looking up handle: %v", err))
		}

		req.Handle = resp.Did
	}

	u := NewUser(req.Handle)
	suggs, err := u.getSuggestedFollows(ctx, s, req.ShowHandles)
	if err != nil {
		return e.String(400, fmt.Sprintf("error getting suggested follows: %v"))
	}

	html := "<html><table><tr><th>suggested did</th><th>bsky profile</th></tr>"
	for _, sugg := range suggs {
		html += fmt.Sprintf("<tr><td>%s</td><td><a href=\"%s\">%s</td></tr>", sugg.SuggestedDid, sugg.BskyUrl, sugg.BskyUrl)
	}
	html += "</table></html>"

	return e.HTML(200, html)
}
