package redlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockNotAcquired = errors.New("failed to acquire lock")
	ErrLockNotHeld     = errors.New("lock not held or expired")
	ErrInvalidTTL      = errors.New("ttl must be greater than zero")
)

// Lock represents an acquired distributed lock
type Lock struct {
	mu       sync.Mutex
	key      string
	value    string
	expiry   time.Time
	managers []*redis.Client
}

func (l *Lock) expired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return time.Now().After(l.expiry)
}

func (l *Lock) setExpiry(expiry time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.expiry = expiry
}

// Redlock implements the Redlock distributed locking algorithm
type Redlock struct {
	clients     []*redis.Client
	quorum      int
	retryCount  int
	retryDelay  time.Duration
	driftFactor float64
}

// NewRedlock creates a new Redlock instance with the given Redis clients
// You should use at least 3 independent Redis instances for production
func NewRedlock(clients []*redis.Client) *Redlock {
	return &Redlock{
		clients:     clients,
		quorum:      len(clients)/2 + 1,
		retryCount:  3,
		retryDelay:  200 * time.Millisecond,
		driftFactor: 0.01, // 1% clock drift
	}
}

// generateValue creates a cryptographically random value for the lock
// This ensures only the lock holder can release it
func generateValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return nil
}

func (r *Redlock) validityDuration(start time.Time, ttl time.Duration) time.Duration {
	elapsed := time.Since(start)
	drift := time.Duration(float64(ttl) * r.driftFactor)
	return ttl - elapsed - drift
}

// Acquire tries to obtain a distributed lock with the given TTL
func (r *Redlock) Acquire(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	if err := validateTTL(ttl); err != nil {
		return nil, err
	}

	value, err := generateValue()
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < r.retryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(r.retryDelay):
			}
		}

		lock, err := r.tryAcquire(ctx, key, value, ttl)
		if err == nil {
			return lock, nil
		}
	}

	return nil, ErrLockNotAcquired
}

// tryAcquire attempts to acquire the lock on all Redis instances
func (r *Redlock) tryAcquire(ctx context.Context, key, value string, ttl time.Duration) (*Lock, error) {
	startTime := time.Now()

	// Try to acquire lock on all instances concurrently
	acquired := make([]bool, len(r.clients))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, client := range r.clients {
		wg.Add(1)
		go func(idx int, c *redis.Client) {
			defer wg.Done()
			ok := r.acquireSingle(ctx, c, key, value, ttl)
			mu.Lock()
			acquired[idx] = ok
			mu.Unlock()
		}(i, client)
	}

	wg.Wait()

	// Count successful acquisitions
	successCount := 0
	for _, ok := range acquired {
		if ok {
			successCount++
		}
	}

	// Calculate elapsed time and check if lock is still valid
	validityTime := r.validityDuration(startTime, ttl)

	// Check if we achieved quorum and lock is still valid
	if successCount >= r.quorum && validityTime > 0 {
		return &Lock{
			key:      key,
			value:    value,
			expiry:   time.Now().Add(validityTime),
			managers: r.clients,
		}, nil
	}

	// Failed to acquire quorum - release any locks we did get
	_ = r.releaseAll(ctx, key, value)
	return nil, ErrLockNotAcquired
}

// acquireSingle tries to acquire lock on a single Redis instance
func (r *Redlock) acquireSingle(ctx context.Context, client *redis.Client, key, value string, ttl time.Duration) bool {
	// SET key value NX PX ttl
	// NX - only set if key does not exist
	// PX - set expiry in milliseconds
	result, err := client.SetNX(ctx, key, value, ttl).Result()
	return err == nil && result
}

// Release unlocks the distributed lock
func (r *Redlock) Release(ctx context.Context, lock *Lock) error {
	if lock == nil {
		return ErrLockNotHeld
	}
	if r.releaseAll(ctx, lock.key, lock.value) < r.quorum {
		return ErrLockNotHeld
	}
	return nil
}

// releaseAll releases the lock from all Redis instances
func (r *Redlock) releaseAll(ctx context.Context, key, value string) int {
	// Lua script ensures we only delete if we hold the lock
	// This prevents deleting a lock that was acquired by another process
	script := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    `

	successCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, client := range r.clients {
		wg.Add(1)
		go func(c *redis.Client) {
			defer wg.Done()
			result, err := c.Eval(ctx, script, []string{key}, value).Int()
			if err == nil && result == 1 {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(client)
	}
	wg.Wait()
	return successCount
}

// Extend attempts to extend the lock's TTL
func (r *Redlock) Extend(ctx context.Context, lock *Lock, ttl time.Duration) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	if lock == nil || lock.expired() {
		return ErrLockNotHeld
	}
	startTime := time.Now()

	// Lua script to extend only if we still hold the lock
	script := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("PEXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
    `

	successCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, client := range r.clients {
		wg.Add(1)
		go func(c *redis.Client) {
			defer wg.Done()
			result, err := c.Eval(ctx, script, []string{lock.key}, lock.value, ttl.Milliseconds()).Int()
			if err == nil && result == 1 {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(client)
	}

	wg.Wait()

	validityTime := r.validityDuration(startTime, ttl)
	if successCount >= r.quorum && validityTime > 0 {
		lock.setExpiry(time.Now().Add(validityTime))
		return nil
	}

	return ErrLockNotHeld
}
