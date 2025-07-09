package peruse

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
)

func (s *Server) startConsumer(ctx context.Context, cancel context.CancelFunc) error {
	defer cancel()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if err := os.WriteFile(s.args.CursorFile, []byte(s.cursor), 0644); err != nil {
				s.logger.Error("error saving cursor", "error", err)
			}
			s.logger.Debug("saving cursor", "seq", s.cursor)
		}
	}()

	u, err := url.Parse(s.args.RelayHost)
	if err != nil {
		return err
	}
	u.Path = "/xrpc/com.atproto.sync.subscribeRepos"

	prevCursor, err := s.loadCursor()
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err)
		}
	} else {
		s.cursor = prevCursor
	}

	if prevCursor != "" {
		u.RawQuery = "cursor=" + prevCursor
	}

	rsc := events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			go s.repoCommit(ctx, evt)
			return nil
		},
	}

	d := websocket.DefaultDialer

	s.logger.Info("connecting to relay", "url", u.String())

	con, _, err := d.Dial(u.String(), http.Header{
		"user-agent": []string{"photocopy/0.0.0"},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to relay: %w", err)
	}

	scheduler := parallel.NewScheduler(400, 10, con.RemoteAddr().String(), rsc.EventHandler)

	if err := events.HandleRepoStream(ctx, con, scheduler, s.logger); err != nil {
		s.logger.Error("repo stream failed", "error", err)
	}

	s.logger.Info("repo stream shut down")

	return nil
}

func (s *Server) repoCommit(ctx context.Context, evt *atproto.SyncSubscribeRepos_Commit) {
	s.cursor = fmt.Sprintf("%d", evt.Seq)

	if evt.TooBig {
		s.logger.Warn("commit too big", "repo", evt.Repo, "seq", evt.Seq)
		return
	}

	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		s.logger.Error("failed to read event repo", "error", err)
		return
	}

	did, err := syntax.ParseDID(evt.Repo)
	if err != nil {
		s.logger.Error("failed to parse did", "error", err)
		return
	}

	for _, op := range evt.Ops {
		collection, rkey, err := syntax.ParseRepoPath(op.Path)
		if err != nil {
			s.logger.Error("invalid path in repo op")
			continue
		}

		ek := repomgr.EventKind(op.Action)

		switch ek {
		case repomgr.EvtKindCreateRecord:
			if op.Cid == nil {
				s.logger.Warn("op missing reccid", "path", op.Path, "action", op.Action)
				continue
			}

			c := (cid.Cid)(*op.Cid)
			reccid, rec, err := r.GetRecordBytes(ctx, op.Path)
			if err != nil {
				s.logger.Error("failed to get record bytes", "error", err, "path", op.Path)
				continue
			}

			if c != reccid {
				s.logger.Warn("reccid mismatch", "from_event", c, "from_blocks", reccid, "path", op.Path)
				continue
			}

			if rec == nil {
				s.logger.Warn("record not found", "reccid", c, "path", op.Path)
				continue
			}

			if err := s.handleCreate(ctx, *rec, evt.Time, evt.Rev, did.String(), collection.String(), rkey.String(), reccid.String(), fmt.Sprintf("%d", evt.Seq)); err != nil {
				s.logger.Error("error handling create event", "error", err)
				continue
			}
		}
	}
}

func (s *Server) loadCursor() (string, error) {
	b, err := os.ReadFile(s.args.CursorFile)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
