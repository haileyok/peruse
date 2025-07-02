package peruse

import (
	"context"
	"time"
)

type SuggestedFollow struct {
	SuggestedDid    string `ch:"suggested_did"`
	BskyUrl         string `ch:"bsky_url"`
	FollowedByCount uint64 `ch:"followed_by_count"`
}

func (u *User) getSuggestedFollows(ctx context.Context, s *Server) ([]SuggestedFollow, error) {
	if !time.Now().After(u.suggestedFollowsExpiresAt) {
		return u.suggestedFollows, nil
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if !time.Now().After(u.suggestedFollowsExpiresAt) {
		return u.suggestedFollows, nil
	}

	var suggestedFollows []SuggestedFollow
	if err := s.conn.Select(ctx, &suggestedFollows, getSuggestedFollowsQuery, u.did); err != nil {
		return nil, err
	}

	u.suggestedFollows = suggestedFollows
	u.suggestedFollowsExpiresAt = time.Now().Add(1 * time.Hour)
	return suggestedFollows, nil
}

const getSuggestedFollowsQuery = `
WITH ? as your_did,
    now() - interval 60 day AS timeframe,
    40 as top_mutual_limit,
    20 as second_level_limit

SELECT
    f.subject as suggested_did,
    concat('https://bsky.app/profile/', f.subject) as bsky_url,
    COUNT(*) as followed_by_count
FROM follow f
WHERE
    f.subject != your_did
    AND f.subject NOT IN (SELECT subject FROM follow WHERE did = your_did)
    AND f.did IN (
        SELECT i.subject_did
        FROM interaction i
        WHERE
            i.kind = 'like'
            AND i.created_at > timeframe
            AND i.did IN (
                SELECT i1.subject_did
                FROM interaction i1, interaction_reverse i2
                WHERE i1.subject_did = i2.did
                AND i2.subject_did = your_did
                AND i2.kind = 'like'
                AND i1.did = your_did
                AND i1.kind = 'like'
                AND i1.created_at > timeframe
                GROUP BY i1.subject_did
                ORDER BY COUNT(*) DESC
                LIMIT top_mutual_limit
            )
        GROUP BY i.subject_did
        ORDER BY COUNT(*) DESC
        LIMIT second_level_limit
    )
GROUP BY f.subject
HAVING COUNT(*) >= 2
ORDER BY followed_by_count DESC
LIMIT 100
	`
