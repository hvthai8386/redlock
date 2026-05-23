package redlock

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})

	cleanup := func() {
		_ = client.Close()
		server.Close()
	}

	return client, cleanup
}

func newTestClients(t *testing.T, count int) []*redis.Client {
	t.Helper()

	clients := make([]*redis.Client, count)
	for i := range clients {
		client, cleanup := newTestClient(t)
		t.Cleanup(cleanup)
		clients[i] = client
	}
	return clients
}

func setLockValue(t *testing.T, client *redis.Client, key, value string) {
	t.Helper()

	if err := client.Set(context.Background(), key, value, time.Minute).Err(); err != nil {
		t.Fatalf("set lock key: %v", err)
	}
}

func TestNewRedlockSetsDefaults(t *testing.T) {
	t.Parallel()

	clients := []*redis.Client{&redis.Client{}, &redis.Client{}, &redis.Client{}}
	r := NewRedlock(clients)

	if len(r.clients) != len(clients) {
		t.Fatalf("expected %d clients, got %d", len(clients), len(r.clients))
	}
	if r.quorum != 2 {
		t.Fatalf("expected quorum 2, got %d", r.quorum)
	}
	if r.retryCount != 3 {
		t.Fatalf("expected retryCount 3, got %d", r.retryCount)
	}
	if r.retryDelay != 200*time.Millisecond {
		t.Fatalf("expected retryDelay 200ms, got %s", r.retryDelay)
	}
	if r.driftFactor != 0.01 {
		t.Fatalf("expected driftFactor 0.01, got %f", r.driftFactor)
	}
}

func TestGenerateValueReturnsRandomHexValue(t *testing.T) {
	t.Parallel()

	first, err := generateValue()
	if err != nil {
		t.Fatalf("generateValue returned error: %v", err)
	}
	second, err := generateValue()
	if err != nil {
		t.Fatalf("generateValue returned error: %v", err)
	}

	if len(first) != 32 {
		t.Fatalf("expected 32 hex characters, got %d", len(first))
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("expected hex value, got %q: %v", first, err)
	}
	if first == second {
		t.Fatal("expected two generated values to differ")
	}
}

func TestValidateTTLAcceptsPositiveDuration(t *testing.T) {
	t.Parallel()

	if err := validateTTL(time.Nanosecond); err != nil {
		t.Fatalf("expected positive ttl to be accepted, got %v", err)
	}
}

func TestAcquireRejectsNonPositiveTTL(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)

	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{name: "zero", ttl: 0},
		{name: "negative", ttl: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lock, err := r.Acquire(context.Background(), "test-lock", tt.ttl)
			if lock != nil {
				t.Fatalf("expected nil lock, got %#v", lock)
			}
			if !errors.Is(err, ErrInvalidTTL) {
				t.Fatalf("expected ErrInvalidTTL, got %v", err)
			}
		})
	}
}

func TestAcquireReturnsLockWhenQuorumAcquires(t *testing.T) {
	t.Parallel()

	clients := newTestClients(t, 3)
	r := NewRedlock(clients)

	lock, err := r.Acquire(context.Background(), "test-lock", time.Minute)
	if err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}
	if lock == nil {
		t.Fatal("expected lock")
	}
	if lock.key != "test-lock" {
		t.Fatalf("expected key test-lock, got %q", lock.key)
	}
	if lock.value == "" {
		t.Fatal("expected lock value to be set")
	}
	if lock.expired() {
		t.Fatal("expected acquired lock not to be expired")
	}

	for _, client := range clients {
		value, err := client.Get(context.Background(), "test-lock").Result()
		if err != nil {
			t.Fatalf("get lock key: %v", err)
		}
		if value != lock.value {
			t.Fatalf("expected redis value %q, got %q", lock.value, value)
		}
	}
}

func TestAcquireReturnsErrLockNotAcquiredWhenLockAlreadyHeld(t *testing.T) {
	t.Parallel()

	clients := newTestClients(t, 3)
	for _, client := range clients {
		setLockValue(t, client, "test-lock", "other-owner")
	}
	r := NewRedlock(clients)
	r.retryCount = 1

	lock, err := r.Acquire(context.Background(), "test-lock", time.Minute)
	if lock != nil {
		t.Fatalf("expected nil lock, got %#v", lock)
	}
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("expected ErrLockNotAcquired, got %v", err)
	}
}

func TestAcquireReturnsContextErrorDuringRetryDelay(t *testing.T) {
	t.Parallel()

	clients := newTestClients(t, 3)
	for _, client := range clients {
		setLockValue(t, client, "test-lock", "other-owner")
	}
	r := NewRedlock(clients)
	r.retryCount = 2
	r.retryDelay = time.Minute

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	lock, err := r.Acquire(ctx, "test-lock", time.Minute)
	if lock != nil {
		t.Fatalf("expected nil lock, got %#v", lock)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestExtendRejectsNonPositiveTTL(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)
	lock := &Lock{
		key:    "test-lock",
		value:  "value",
		expiry: time.Now().Add(time.Minute),
	}

	tests := []struct {
		name string
		ttl  time.Duration
	}{
		{name: "zero", ttl: 0},
		{name: "negative", ttl: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := r.Extend(context.Background(), lock, tt.ttl)
			if !errors.Is(err, ErrInvalidTTL) {
				t.Fatalf("expected ErrInvalidTTL, got %v", err)
			}
		})
	}
}

func TestTryAcquireReleasesPartialAcquisitionWhenQuorumFails(t *testing.T) {
	t.Parallel()

	clients := newTestClients(t, 3)
	setLockValue(t, clients[1], "test-lock", "other-owner")
	setLockValue(t, clients[2], "test-lock", "other-owner")
	r := NewRedlock(clients)

	lock, err := r.tryAcquire(context.Background(), "test-lock", "owner", time.Minute)
	if lock != nil {
		t.Fatalf("expected nil lock, got %#v", lock)
	}
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("expected ErrLockNotAcquired, got %v", err)
	}

	exists, err := clients[0].Exists(context.Background(), "test-lock").Result()
	if err != nil {
		t.Fatalf("check partial lock key: %v", err)
	}
	if exists != 0 {
		t.Fatalf("expected partial acquisition to be released, exists=%d", exists)
	}
	for _, client := range clients[1:] {
		value, err := client.Get(context.Background(), "test-lock").Result()
		if err != nil {
			t.Fatalf("get existing lock key: %v", err)
		}
		if value != "other-owner" {
			t.Fatalf("expected other-owner to remain, got %q", value)
		}
	}
}

func TestAcquireSingleSetsLockOnlyWhenKeyDoesNotExist(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestClient(t)
	t.Cleanup(cleanup)
	r := NewRedlock([]*redis.Client{client})

	if !r.acquireSingle(context.Background(), client, "test-lock", "owner", time.Minute) {
		t.Fatal("expected first acquireSingle call to acquire lock")
	}
	if r.acquireSingle(context.Background(), client, "test-lock", "second-owner", time.Minute) {
		t.Fatal("expected second acquireSingle call to fail for existing lock")
	}

	value, err := client.Get(context.Background(), "test-lock").Result()
	if err != nil {
		t.Fatalf("get lock key: %v", err)
	}
	if value != "owner" {
		t.Fatalf("expected original owner to remain, got %q", value)
	}
}

func TestValidityDurationSubtractsElapsedAndDrift(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)
	ttl := 100 * time.Millisecond
	start := time.Now().Add(-25 * time.Millisecond)

	validity := r.validityDuration(start, ttl)

	if validity <= 0 {
		t.Fatalf("expected positive validity, got %s", validity)
	}
	if validity >= ttl {
		t.Fatalf("expected validity less than ttl, got %s", validity)
	}
}

func TestValidityDurationExpiresWhenElapsedConsumesTTLAndDrift(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)
	ttl := 100 * time.Millisecond
	start := time.Now().Add(-ttl)

	validity := r.validityDuration(start, ttl)

	if validity > 0 {
		t.Fatalf("expected non-positive validity, got %s", validity)
	}
}

func TestLockExpiryAccessIsSafeDuringConcurrentExtend(t *testing.T) {
	t.Parallel()

	lock := &Lock{
		key:    "test-lock",
		value:  "value",
		expiry: time.Now().Add(time.Minute),
	}

	const goroutines = 20
	done := make(chan struct{}, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() {
				done <- struct{}{}
			}()

			for j := 0; j < 100; j++ {
				lock.setExpiry(time.Now().Add(time.Minute))
				_ = lock.expired()
			}
		}()
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestReleaseReturnsNilWhenQuorumDeletesLock(t *testing.T) {
	t.Parallel()

	client1, cleanup1 := newTestClient(t)
	t.Cleanup(cleanup1)

	client2, cleanup2 := newTestClient(t)
	t.Cleanup(cleanup2)

	client3, cleanup3 := newTestClient(t)
	t.Cleanup(cleanup3)

	r := NewRedlock([]*redis.Client{client1, client2, client3})
	lock := &Lock{
		key:    "test-lock",
		value:  "owner",
		expiry: time.Now().Add(time.Minute),
	}

	for _, client := range []*redis.Client{client1, client2, client3} {
		if err := client.Set(context.Background(), lock.key, lock.value, time.Minute).Err(); err != nil {
			t.Fatalf("set lock key: %v", err)
		}
	}

	if err := r.Release(context.Background(), lock); err != nil {
		t.Fatalf("Release returned error: %v", err)
	}

	for _, client := range []*redis.Client{client1, client2, client3} {
		exists, err := client.Exists(context.Background(), lock.key).Result()
		if err != nil {
			t.Fatalf("check lock key: %v", err)
		}
		if exists != 0 {
			t.Fatalf("expected lock key to be deleted, exists=%d", exists)
		}
	}
}

func TestReleaseReturnsErrLockNotHeldForNilLock(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)

	err := r.Release(context.Background(), nil)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld, got %v", err)
	}
}

func TestReleaseReturnsErrLockNotHeldWhenQuorumDoesNotDelete(t *testing.T) {
	t.Parallel()

	client1, cleanup1 := newTestClient(t)
	t.Cleanup(cleanup1)

	client2, cleanup2 := newTestClient(t)
	t.Cleanup(cleanup2)

	client3, cleanup3 := newTestClient(t)
	t.Cleanup(cleanup3)

	r := NewRedlock([]*redis.Client{client1, client2, client3})
	lock := &Lock{
		key:    "test-lock",
		value:  "owner",
		expiry: time.Now().Add(time.Minute),
	}

	if err := client1.Set(context.Background(), lock.key, "different-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock key: %v", err)
	}

	err := r.Release(context.Background(), lock)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld, got %v", err)
	}
}

func TestReleaseAllReturnsSuccessfulDeleteCount(t *testing.T) {
	t.Parallel()

	clients := newTestClients(t, 3)
	setLockValue(t, clients[0], "test-lock", "owner")
	setLockValue(t, clients[1], "test-lock", "owner")
	setLockValue(t, clients[2], "test-lock", "other-owner")
	r := NewRedlock(clients)

	count := r.releaseAll(context.Background(), "test-lock", "owner")
	if count != 2 {
		t.Fatalf("expected 2 successful deletes, got %d", count)
	}

	for _, client := range clients[:2] {
		exists, err := client.Exists(context.Background(), "test-lock").Result()
		if err != nil {
			t.Fatalf("check deleted lock key: %v", err)
		}
		if exists != 0 {
			t.Fatalf("expected matching lock to be deleted, exists=%d", exists)
		}
	}
	value, err := clients[2].Get(context.Background(), "test-lock").Result()
	if err != nil {
		t.Fatalf("get non-matching lock key: %v", err)
	}
	if value != "other-owner" {
		t.Fatalf("expected non-matching lock to remain, got %q", value)
	}
}

func TestExtendUpdatesExpiryWhenQuorumExtendsLock(t *testing.T) {
	t.Parallel()

	client1, cleanup1 := newTestClient(t)
	t.Cleanup(cleanup1)

	client2, cleanup2 := newTestClient(t)
	t.Cleanup(cleanup2)

	client3, cleanup3 := newTestClient(t)
	t.Cleanup(cleanup3)

	r := NewRedlock([]*redis.Client{client1, client2, client3})
	lock := &Lock{
		key:    "test-lock",
		value:  "owner",
		expiry: time.Now().Add(time.Minute),
	}

	for _, client := range []*redis.Client{client1, client2, client3} {
		if err := client.Set(context.Background(), lock.key, lock.value, time.Minute).Err(); err != nil {
			t.Fatalf("set lock key: %v", err)
		}
	}

	if err := r.Extend(context.Background(), lock, time.Minute); err != nil {
		t.Fatalf("Extend returned error: %v", err)
	}

	if lock.expired() {
		t.Fatal("expected lock not to be expired after extend")
	}
}

func TestExtendReturnsErrLockNotHeldForNilLock(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)

	err := r.Extend(context.Background(), nil, time.Minute)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld, got %v", err)
	}
}

func TestExtendReturnsErrLockNotHeldForExpiredLock(t *testing.T) {
	t.Parallel()

	r := NewRedlock(nil)
	lock := &Lock{
		key:    "test-lock",
		value:  "owner",
		expiry: time.Now().Add(-time.Nanosecond),
	}

	err := r.Extend(context.Background(), lock, time.Minute)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld, got %v", err)
	}
}

func TestExtendReturnsErrLockNotHeldWhenQuorumDoesNotExtend(t *testing.T) {
	t.Parallel()

	client1, cleanup1 := newTestClient(t)
	t.Cleanup(cleanup1)

	client2, cleanup2 := newTestClient(t)
	t.Cleanup(cleanup2)

	client3, cleanup3 := newTestClient(t)
	t.Cleanup(cleanup3)

	r := NewRedlock([]*redis.Client{client1, client2, client3})
	lock := &Lock{
		key:    "test-lock",
		value:  "owner",
		expiry: time.Now().Add(time.Minute),
	}

	if err := client1.Set(context.Background(), lock.key, "different-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock key: %v", err)
	}

	err := r.Extend(context.Background(), lock, time.Minute)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld, got %v", err)
	}
}
