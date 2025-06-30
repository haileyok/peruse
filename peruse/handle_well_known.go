package peruse

import "github.com/labstack/echo/v4"

type wellKnownResponse struct {
	Context []string `json:"@context"`
	Id      string   `json:"id"`
	Service []wellKnownService
}

type wellKnownService struct {
	Id              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

func (s *Server) handleWellKnown(e echo.Context) error {
	return e.JSON(200, wellKnownResponse{
		Context: []string{"https://www.w3.org/ns/did/v1"},
		Id:      s.args.ServiceDid,
		Service: []wellKnownService{
			{
				Id:              "#bsky_fg",
				Type:            "BskyFeedGenerator",
				ServiceEndpoint: s.args.ServiceEndpoint,
			},
		},
	})
}
