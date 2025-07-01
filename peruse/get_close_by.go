package peruse

import (
	"context"
	"time"
)

type CloseBy struct {
	SuggestedDid      string `ch:"suggested_did"`
	BskyUrl           string `ch:"bsky_url"`
	InteractionScore  uint64 `ch:"interaction_score"`
	InteractedByCount uint64 `ch:"interacted_by_count"`
	ConnectionType    string `ch:"connection_type"`
	BlendedScore      uint64 `ch:"blended_score"`
}

const (
	CloseByExistingConnectionWeight = 1
	NewDiscoveryWeight              = 1
	TopMutualLimit                  = 500
)

func (u *User) getCloseBy(ctx context.Context, s *Server) ([]CloseBy, error) {
	// TODO: this "if you have more than 10" feels a little bit too low?
	if !time.Now().After(u.closeByExpiresAt) && len(u.following) > 10 {
		return u.closeBy, nil
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if !time.Now().After(u.closeByExpiresAt) && len(u.following) > 10 {
		return u.closeBy, nil
	}

	var closeBy []CloseBy
	if err := s.conn.Select(ctx, &closeBy, getCloseByQuery, u.did, CloseByExistingConnectionWeight, NewDiscoveryWeight, TopMutualLimit); err != nil {
		return nil, err
	}

	u.closeByExpiresAt = time.Now().Add(1 * time.Hour)

	return closeBy, nil
}

var getCloseByQuery = `
WITH ? as your_did,
    now() - interval 14 day AS timeframe,
    ? as existing_connection_weight,
    ? as new_discovery_weight,
    ? as top_mutual_limit

SELECT
    i.subject_did as suggested_did,
    concat('https://bsky.app/profile/', suggested_did) as bsky_url,
    COUNT(*) as interaction_score,
    COUNT(DISTINCT i.did) as interacted_by_count,
    CASE
        WHEN i.subject_did IN (
            SELECT subject_did FROM interaction WHERE did = your_did AND created_at > timeframe
            UNION ALL
            SELECT subject FROM follow WHERE did = your_did
        )
        THEN 'existing_connection'
        ELSE 'new_discovery'
    END as connection_type,
    COUNT(*) * CASE
        WHEN i.subject_did IN (
            SELECT subject_did FROM interaction WHERE did = your_did AND created_at > timeframe
            UNION ALL
            SELECT subject FROM follow WHERE did = your_did
        )
        THEN existing_connection_weight
        ELSE new_discovery_weight
    END as blended_score
FROM interaction i
WHERE
    i.kind = 'like'
    AND i.created_at > timeframe
    AND i.subject_did != your_did
    AND i.did IN (
        SELECT i1.subject_did
        FROM interaction i1
        INNER JOIN interaction_reverse i2 ON (
            i1.subject_did = i2.did
            AND i2.subject_did = your_did
            AND i2.kind = 'like'
            AND i1.created_at > timeframe
        )
        WHERE
            i1.did = your_did
            AND i1.kind = 'like'
            AND i1.created_at > timeframe
        GROUP BY i1.subject_did
        ORDER BY COUNT(*) DESC
        LIMIT top_mutual_limit
    )
GROUP BY i.subject_did
ORDER BY blended_score DESC
LIMIT 500
	`
