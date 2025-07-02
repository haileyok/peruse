package peruse

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type UserManager struct {
	mu    sync.RWMutex
	users *lru.Cache[string, *User]
}

func NewUserManager() *UserManager {
	uc, _ := lru.New[string, *User](20_000)
	return &UserManager{
		users: uc,
	}
}

func (um *UserManager) getUser(did string) *User {
	um.mu.RLock()
	u, ok := um.users.Get(did)
	um.mu.RUnlock()
	if ok {
		return u
	}

	um.mu.Lock()
	defer um.mu.Unlock()

	if u, ok := um.users.Get(did); ok {
		return u
	}

	u = NewUser(did)
	um.users.Add(did, u)

	return u
}

type User struct {
	mu sync.Mutex

	did string

	following          []string
	followingExpiresAt time.Time

	closeBy          []CloseBy
	closeByExpiresAt time.Time

	suggestedFollows          []SuggestedFollow
	suggestedFollowsExpiresAt time.Time
}

func NewUser(did string) *User {
	return &User{
		did: did,
	}
}

func (u *User) getFollowing() []string {
	return nil
}
