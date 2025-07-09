package peruse

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

func (s *Server) handleCreate(ctx context.Context, recb []byte, indexedAt, rev, did, collection, rkey, cid, seq string) error {
	iat, err := dateparse.ParseAny(indexedAt)
	if err != nil {
		return err
	}

	switch collection {
	case "app.bsky.feed.post":
		return s.handleCreatePost(ctx, rev, recb, uriFromParts(did, collection, rkey), did, collection, rkey, cid, iat)
	case "app.bsky.feed.like":
		return s.handleCreateLike(ctx, rev, recb, uriFromParts(did, collection, rkey), did, collection, rkey, cid, iat)
	case "app.bsky.feed.repost":
		return s.handleCreateRepost(ctx, rev, recb, uriFromParts(did, collection, rkey), did, collection, rkey, cid, iat)
	default:
		return nil
	}
}

func (s *Server) handleCreatePost(ctx context.Context, rev string, recb []byte, uri, did, collection, rkey, cid string, indexedAt time.Time) error {
	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	for fname, f := range s.feeds {
		go func() {
			if err := f.OnPost(ctx, &rec, uri, did, rkey, cid, indexedAt); err != nil {
				s.logger.Error("error running on post", "feed", fname, "error", err)
			}
		}()
	}

	return nil
}

func (s *Server) handleCreateLike(ctx context.Context, rev string, recb []byte, uri, did, collection, rkey, cid string, indexedAt time.Time) error {
	var rec bsky.FeedLike
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	for fname, f := range s.feeds {
		go func() {
			if err := f.OnLike(ctx, &rec, uri, did, rkey, cid, indexedAt); err != nil {
				s.logger.Error("error running on like", "feed", fname, "error", err)
			}
		}()
	}

	return nil
}

func (s *Server) handleCreateRepost(ctx context.Context, rev string, recb []byte, uri, did, collection, rkey, cid string, indexedAt time.Time) error {
	var rec bsky.FeedRepost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	for fname, f := range s.feeds {
		go func() {
			if err := f.OnRepost(ctx, &rec, uri, did, rkey, cid, indexedAt); err != nil {
				s.logger.Error("error running on repost", "feed", fname, "error", err)
			}
		}()
	}

	return nil
}

func uriFromParts(did string, collection string, rkey string) string {
	return "at://" + did + "/" + collection + "/" + rkey
}

func parseTimeFromRecord(rec any, rkey string) (*time.Time, error) {
	var rkeyTime time.Time
	if rkey != "self" {
		rt, err := syntax.ParseTID(rkey)
		if err == nil {
			rkeyTime = rt.Time()
		}
	}
	switch rec := rec.(type) {
	case *bsky.FeedPost:
		t, err := dateparse.ParseAny(rec.CreatedAt)
		if err != nil {
			return nil, err
		}

		if inRange(t) {
			return &t, nil
		}

		if rkeyTime.IsZero() || !inRange(rkeyTime) {
			return timePtr(time.Now()), nil
		}

		return &rkeyTime, nil
	case *bsky.FeedRepost:
		t, err := dateparse.ParseAny(rec.CreatedAt)
		if err != nil {
			return nil, err
		}

		if inRange(t) {
			return timePtr(t), nil
		}

		if rkeyTime.IsZero() {
			return nil, fmt.Errorf("failed to get a useful timestamp from record")
		}

		return &rkeyTime, nil
	case *bsky.FeedLike:
		t, err := dateparse.ParseAny(rec.CreatedAt)
		if err != nil {
			return nil, err
		}

		if inRange(t) {
			return timePtr(t), nil
		}

		if rkeyTime.IsZero() {
			return nil, fmt.Errorf("failed to get a useful timestamp from record")
		}

		return &rkeyTime, nil
	case *bsky.ActorProfile:
		// We can't really trust the createdat in the profile record anyway, and its very possible its missing. just use iat for this one
		return timePtr(time.Now()), nil
	case *bsky.FeedGenerator:
		if !rkeyTime.IsZero() && inRange(rkeyTime) {
			return &rkeyTime, nil
		}
		return timePtr(time.Now()), nil
	default:
		if !rkeyTime.IsZero() && inRange(rkeyTime) {
			return &rkeyTime, nil
		}
		return timePtr(time.Now()), nil
	}
}

func inRange(t time.Time) bool {
	now := time.Now()
	if t.Before(now) {
		return now.Sub(t) <= time.Hour*24*365*5
	}
	return t.Sub(now) <= time.Hour*24*200
}

func timePtr(t time.Time) *time.Time {
	return &t
}
