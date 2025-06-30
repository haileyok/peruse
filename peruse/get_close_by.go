package peruse

import (
	"context"
	"time"
)

type CloseBy struct {
	Did                   string  `ch:"did"`
	TheirLikes            uint64  `ch:"their_likes"`
	MyLikes               uint64  `ch:"my_likes"`
	TheirReplies          uint64  `ch:"their_replies"`
	MyReplies             uint64  `ch:"my_replies"`
	FriendConnectionScore float64 `ch:"friend_connection_score"`
	ClosenessScore        float64 `ch:"closeness_score"`
	InteractionType       string  `ch:"interaction_type"`
}

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
	if err := s.conn.Select(ctx, &closeBy, getCloseByQuery, u.did); err != nil {
		return nil, err
	}

	u.closeByExpiresAt = time.Now().Add(1 * time.Hour)

	return closeBy, nil
}

var getCloseByQuery = `
WITH
  ? AS my_did

SELECT
  all_dids.did AS did,
  coalesce(likes.their_likes, 0) AS their_likes,
  coalesce(likes.my_likes, 0) AS my_likes,
  coalesce(replies.their_replies, 0) AS their_replies,
  coalesce(replies.my_replies, 0) AS my_replies,
  coalesce(friends.friend_connection_score, 0) AS friend_connection_score,

  (coalesce(likes.their_likes, 0) + coalesce(likes.my_likes, 0)) * 1.0 +
  (coalesce(replies.their_replies, 0) + coalesce(replies.my_replies, 0)) * 2.0 +
  coalesce(friends.friend_connection_score, 0) AS closeness_score,

  multiIf(
    coalesce(likes.their_likes, 0) > 0 AND coalesce(likes.my_likes, 0) > 0, 'mutual_likes',
    coalesce(replies.their_replies, 0) > 0 AND coalesce(replies.my_replies, 0) > 0, 'mutual_replies',
    coalesce(likes.my_likes, 0) > 0 OR coalesce(replies.my_replies, 0) > 0, 'one_way_from_me',
    coalesce(likes.their_likes, 0) > 0 OR coalesce(replies.their_replies, 0) > 0, 'one_way_to_me',
    coalesce(friends.friend_connection_score, 0) > 0, 'friend_of_friends',
    'unknown'
  ) AS interaction_type

FROM (
  SELECT did FROM (
    SELECT subject_did AS did FROM default.interaction WHERE did = my_did AND kind = 'like'
    UNION DISTINCT
    SELECT did FROM default.interaction WHERE subject_did = my_did AND kind = 'like'
    UNION DISTINCT
    SELECT parent_did AS did FROM default.post WHERE did = my_did AND parent_did IS NOT NULL
    UNION DISTINCT
    SELECT did FROM default.post WHERE parent_did = my_did
    UNION DISTINCT
    SELECT i.subject_did AS did
    FROM default.interaction i
    WHERE i.kind = 'like'
      AND i.subject_did != my_did
      AND i.did IN (
        SELECT did FROM (
          SELECT
            coalesce(top_l.did, top_r.did) AS did,
            (coalesce(top_l.their_likes, 0) + coalesce(top_l.my_likes, 0)) * 1.0 +
            (coalesce(top_r.their_replies, 0) + coalesce(top_r.my_replies, 0)) * 2.0 AS friend_score
          FROM (
            SELECT
              lm.them AS did,
              lm.their_likes,
              il.my_likes
            FROM (
              SELECT did AS them, count(*) AS their_likes
              FROM default.interaction
              WHERE subject_did = my_did AND kind = 'like'
              GROUP BY did
            ) AS lm
            INNER JOIN (
              SELECT subject_did AS them, count(*) as my_likes
              FROM default.interaction
              WHERE did = my_did AND kind = 'like'
              GROUP BY subject_did
            ) AS il ON lm.them = il.them
          ) AS top_l
          FULL OUTER JOIN (
            SELECT
              replies_to_you.them AS did,
              replies_to_you.their_replies,
              replies_to_them.my_replies
            FROM (
              SELECT did AS them, count(*) AS their_replies
              FROM default.post
              WHERE parent_did = my_did
              GROUP BY did
            ) AS replies_to_you
            INNER JOIN (
              SELECT parent_did AS them, count(*) AS my_replies
              FROM default.post
              WHERE did = my_did
              GROUP BY parent_did
            ) AS replies_to_them ON replies_to_you.them = replies_to_them.them
          ) AS top_r ON top_l.did = top_r.did
          ORDER BY friend_score DESC
          LIMIT 50
        )
      )
      AND i.subject_did NOT IN (
        SELECT subject_did FROM default.interaction WHERE did = my_did
        UNION DISTINCT
        SELECT did FROM default.interaction WHERE subject_did = my_did
        UNION DISTINCT
        SELECT parent_did FROM default.post WHERE did = my_did AND parent_did IS NOT NULL
        UNION DISTINCT
        SELECT did FROM default.post WHERE parent_did = my_did
      )
    GROUP BY i.subject_did
    HAVING count(*) >= 3
  )
) AS all_dids

LEFT JOIN (
  SELECT
    did,
    sum(their_likes) as their_likes,
    sum(my_likes) as my_likes
  FROM (
    SELECT
      subject_did AS did,
      0 as their_likes,
      count(*) as my_likes
    FROM default.interaction
    WHERE did = my_did AND kind = 'like'
    GROUP BY subject_did

    UNION ALL

    SELECT
      did,
      count(*) AS their_likes,
      0 as my_likes
    FROM default.interaction
    WHERE subject_did = my_did AND kind = 'like'
    GROUP BY did
  )
  GROUP BY did
) AS likes ON all_dids.did = likes.did

LEFT JOIN (
  SELECT
    did,
    sum(their_replies) as their_replies,
    sum(my_replies) as my_replies
  FROM (
    SELECT
      parent_did AS did,
      0 as their_replies,
      count(*) AS my_replies
    FROM default.post
    WHERE did = my_did AND parent_did IS NOT NULL
    GROUP BY parent_did

    UNION ALL

    SELECT
      did,
      count(*) AS their_replies,
      0 as my_replies
    FROM default.post
    WHERE parent_did = my_did
    GROUP BY did
  )
  GROUP BY did
) AS replies ON all_dids.did = replies.did

LEFT JOIN (
  SELECT
    i.subject_did AS did,
    count(*) * 0.3 AS friend_connection_score
  FROM default.interaction i
  WHERE i.kind = 'like'
    AND i.subject_did != my_did
    AND i.did IN (
      SELECT did FROM (
        SELECT
          coalesce(top_l.did, top_r.did) AS did,
          (coalesce(top_l.their_likes, 0) + coalesce(top_l.my_likes, 0)) * 1.0 +
          (coalesce(top_r.their_replies, 0) + coalesce(top_r.my_replies, 0)) * 2.0 AS friend_score
        FROM (
          SELECT
            lm.them AS did,
            lm.their_likes,
            il.my_likes
          FROM (
            SELECT did AS them, count(*) AS their_likes
            FROM default.interaction
            WHERE subject_did = my_did AND kind = 'like'
            GROUP BY did
          ) AS lm
          INNER JOIN (
            SELECT subject_did AS them, count(*) as my_likes
            FROM default.interaction
            WHERE did = my_did AND kind = 'like'
            GROUP BY subject_did
          ) AS il ON lm.them = il.them
        ) AS top_l
        FULL OUTER JOIN (
          SELECT
            replies_to_you.them AS did,
            replies_to_you.their_replies,
            replies_to_them.my_replies
          FROM (
            SELECT did AS them, count(*) AS their_replies
            FROM default.post
            WHERE parent_did = my_did
            GROUP BY did
          ) AS replies_to_you
          INNER JOIN (
            SELECT parent_did AS them, count(*) AS my_replies
            FROM default.post
            WHERE did = my_did
            GROUP BY parent_did
          ) AS replies_to_them ON replies_to_you.them = replies_to_them.them
        ) AS top_r ON top_l.did = top_r.did
        ORDER BY friend_score DESC
        LIMIT 50
      )
    )
    AND i.subject_did NOT IN (
      SELECT subject_did FROM default.interaction WHERE did = my_did
      UNION DISTINCT
      SELECT did FROM default.interaction WHERE subject_did = my_did
      UNION DISTINCT
      SELECT parent_did FROM default.post WHERE did = my_did AND parent_did IS NOT NULL
      UNION DISTINCT
      SELECT did FROM default.post WHERE parent_did = my_did
    )
  GROUP BY i.subject_did
  HAVING count(*) >= 3
) AS friends ON all_dids.did = friends.did

WHERE all_dids.did IS NOT NULL
ORDER BY closeness_score DESC
LIMIT 1000 
	`
