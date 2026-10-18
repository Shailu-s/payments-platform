package auth

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

const (
	// A revoked key keeps working on other processes for up to this long.
	DefaultCacheTTL = 30 * time.Second
	// Bounds memory: about 200 bytes per key.
	DefaultCacheSize = 10000
)

// Cache verifies keys like Verify, remembering live keys for a TTL so a
// request does not spend a database round trip on its key. Entries are keyed
// by the key's hash; the plaintext is never stored.
//
// Only live keys are cached. A cached unknown key would let anyone with a
// stream of random keys evict the real ones, and a key inserted a moment
// ago would be refused until the entry expired.
type Cache struct {
	db  Querier
	ttl time.Duration
	max int
	now func() time.Time

	mu sync.Mutex
	// Every entry has the same TTL, so insertion order is expiry order: the
	// front of order is always the next to expire.
	order   *list.List
	entries map[string]*list.Element
	loading map[string]*load
	// Bumped by Revoke, so a lookup that started before it does not cache
	// the key it read.
	generation uint64
}

type cacheEntry struct {
	hash    string
	key     Key
	expires time.Time
}

type load struct {
	done chan struct{}
	key  Key
	err  error
}

func NewCache(db Querier, ttl time.Duration, size int) *Cache {
	return &Cache{
		db:      db,
		ttl:     ttl,
		max:     size,
		now:     time.Now,
		order:   list.New(),
		entries: map[string]*list.Element{},
		loading: map[string]*load{},
	}
}

// Verify returns the key for plaintext, from memory if it was verified in
// the last TTL. Concurrent misses for one key share a single query.
func (c *Cache) Verify(ctx context.Context, plaintext string) (Key, error) {
	if !wellFormed(plaintext) {
		return Key{}, ErrInvalidKey
	}
	hash := hashKey(plaintext)

	for {
		c.mu.Lock()
		if el, ok := c.entries[hash]; ok {
			entry := el.Value.(*cacheEntry)
			if c.now().Before(entry.expires) {
				c.mu.Unlock()
				return entry.key, nil
			}
			c.remove(el)
		}
		if l, ok := c.loading[hash]; ok {
			c.mu.Unlock()
			select {
			case <-l.done:
			case <-ctx.Done():
				return Key{}, ctx.Err()
			}
			// The leader's deadline is not ours: if it ran out, look again.
			if isContextErr(l.err) && ctx.Err() == nil {
				continue
			}
			return l.key, l.err
		}
		l := &load{done: make(chan struct{})}
		c.loading[hash] = l
		generation := c.generation
		c.mu.Unlock()

		l.key, l.err = lookup(ctx, c.db, hash)

		c.mu.Lock()
		delete(c.loading, hash)
		if l.err == nil && generation == c.generation {
			c.insert(hash, l.key)
		}
		c.mu.Unlock()
		close(l.done)
		return l.key, l.err
	}
}

// Revoke revokes the key and forgets it here at once. Other processes keep
// accepting it until their entry expires, up to the TTL.
func (c *Cache) Revoke(ctx context.Context, db Execer, id string) error {
	err := Revoke(ctx, db, id)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	for el := c.order.Front(); el != nil; {
		next := el.Next()
		if el.Value.(*cacheEntry).key.ID == id {
			c.remove(el)
		}
		el = next
	}
	return err
}

// insert adds a live key, dropping expired entries and then, if still full,
// the oldest. The caller holds mu.
func (c *Cache) insert(hash string, key Key) {
	now := c.now()
	for el := c.order.Front(); el != nil && !now.Before(el.Value.(*cacheEntry).expires); el = c.order.Front() {
		c.remove(el)
	}
	for c.order.Len() >= c.max && c.order.Len() > 0 {
		c.remove(c.order.Front())
	}
	if el, ok := c.entries[hash]; ok {
		c.remove(el)
	}
	c.entries[hash] = c.order.PushBack(&cacheEntry{hash: hash, key: key, expires: now.Add(c.ttl)})
}

func (c *Cache) remove(el *list.Element) {
	delete(c.entries, el.Value.(*cacheEntry).hash)
	c.order.Remove(el)
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
