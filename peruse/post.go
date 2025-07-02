package peruse

import (
	"context"
	"strings"

	"github.com/haileyok/photocopy/models"
)

func (s *Server) getPostsForDidsChronological(ctx context.Context, dids []string, cursor string) ([]models.Post, error) {
	var posts []models.Post
	if err := s.conn.Select(ctx, &posts, `
		SELECT uri
		FROM default.post
		WHERE rkey < ?
		AND did IN (?)
		AND parent_uri = ''
		ORDER BY created_at DESC
		LIMIT 50
		`, cursor, dids); err != nil {
		return nil, err
	}
	return posts, nil
}

func modelPostsToFeedItems(posts []models.Post) ([]FeedPostItem, string) {
	var fpis []FeedPostItem
	var cursor string
	for i, p := range posts {
		fpis = append(fpis, FeedPostItem{
			Post: p.Uri,
		})
		if i == len(posts)-1 {
			pts := strings.Split(p.Uri, "/")
			cursor = pts[len(pts)-1]
		}
	}
	return fpis, cursor
}
