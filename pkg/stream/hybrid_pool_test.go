package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/rtmpserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCommander struct {
	mu   sync.Mutex
	puts int
	url  string
	err  error
}

func (f *fakeCommander) SendRequest(reqType client.RequestType, data *client.Request) func(time.Duration) (*client.Response, error) {
	f.mu.Lock()
	f.puts++
	if data != nil && data.Streaming != nil && data.Streaming.RtmpUrl != nil {
		f.url = *data.Streaming.RtmpUrl
	}
	err := f.err
	f.mu.Unlock()

	return func(time.Duration) (*client.Response, error) {
		if err != nil {
			return nil, err
		}
		code := int32(200)
		return &client.Response{StatusCode: &code}, nil
	}
}

func (f *fakeCommander) putCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

func (f *fakeCommander) putURL() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.url
}

type idleRelay struct{}

func (idleRelay) Start(context.Context) error { return nil }
func (idleRelay) Stop() error                 { return nil }

func newRecoveryPool(t *testing.T, fake *fakeCommander) *HybridStreamPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stateManager := baby.NewStateManager()
	p := NewHybridStreamPool(HybridStreamPoolConfig{
		BabyUID:      "baby-1",
		LocalURL:     "rtmp://192.168.60.157:1935/local/baby-1",
		StateManager: stateManager,
		RTMPServer:   rtmpserver.NewRTMPServer(stateManager),
		Health:       NewLocalHealthRegistry(),
	})
	p.conn = fake
	p.started = true
	p.ctx = ctx
	p.localPublishWait = 40 * time.Millisecond
	p.remoteRelay = idleRelay{}
	return p
}

func TestStallWithoutRemoteReputsLocalOnce(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)
	p.localActive = true
	p.remoteActive = false
	p.activeSource = HybridSourceLocal

	p.onStallDetected(HybridSourceLocal)
	require.Eventually(t, func() bool {
		return fake.putCount() == 1
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, p.localURL, fake.putURL())

	p.onStallDetected(HybridSourceLocal)
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, 1, fake.putCount())
}

func TestWatchdogReputsWhenMQTTFlagWouldStillLookAlive(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)
	// Stale in-memory flag. Publisher count on the RTMP server is still zero.
	p.localActive = true
	p.remoteActive = true
	p.absentSince = time.Now().Add(-LocalStarveAfter - time.Second)

	p.checkLocalHealth(time.Now())

	require.Eventually(t, func() bool {
		return fake.putCount() == 1
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, p.localURL, fake.putURL())
	assert.False(t, p.IsLocalRTMPHealthy())
}

func TestWatchdogWaitsOutShortAbsence(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)
	p.absentSince = time.Now()

	p.checkLocalHealth(time.Now())
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, 0, fake.putCount())
}

func TestWatchdogReputsSilentPublisher(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)
	p.localActive = true
	stale := time.Now().Add(-LocalStarveAfter - time.Second)
	p.localStats = func() (int, time.Time) {
		return 1, stale
	}

	p.checkLocalHealth(time.Now())

	require.Eventually(t, func() bool {
		return fake.putCount() == 1
	}, time.Second, 5*time.Millisecond)
}

func TestWatchdogKeepsFreshLocalPublish(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)
	fresh := time.Now()
	p.localStats = func() (int, time.Time) {
		return 1, fresh
	}

	p.checkLocalHealth(time.Now())
	time.Sleep(60 * time.Millisecond)

	assert.Equal(t, 0, fake.putCount())
	healthy, reports := p.health.Snapshot()
	assert.True(t, healthy)
	require.Len(t, reports, 1)
	assert.Equal(t, 1, reports[0].PublisherCount)
	assert.True(t, p.IsLocalRTMPHealthy())
}

func TestFreshLocalMediaPrefersLocalOverRemote(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)
	p.activeSource = HybridSourceRemote
	p.localStats = func() (int, time.Time) {
		return 1, time.Now()
	}
	p.rtmpServer.GetOrCreateBroadcaster(p.babyUID)
	p.rtmpServer.GetBroadcaster(p.babyUID).SetActiveSource(HybridSourceRemote)

	p.checkLocalHealth(time.Now())

	assert.Equal(t, HybridSourceLocal, p.GetActiveSource())
	assert.True(t, p.rtmpServer.GetBroadcaster(p.babyUID).IsPendingSwitch())
	time.Sleep(40 * time.Millisecond)
	assert.Equal(t, 0, fake.putCount())
}

func TestPublisherDisconnectReputsLocal(t *testing.T) {
	fake := &fakeCommander{}
	p := newRecoveryPool(t, fake)

	p.onLocalPublisherCount("someone-else", 0)
	time.Sleep(40 * time.Millisecond)
	assert.Equal(t, 0, fake.putCount())

	p.onLocalPublisherCount(p.babyUID, 0)
	require.Eventually(t, func() bool {
		return fake.putCount() == 1
	}, time.Second, 5*time.Millisecond)

	p.onLocalPublisherCount(p.babyUID, 0)
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, 1, fake.putCount())
}

func TestLocalRetryBackoffJitterAndConnectionLimit(t *testing.T) {
	p := newRecoveryPool(t, &fakeCommander{})

	before := time.Now()
	p.noteLocalAttempt(ErrPublisherTimeout)
	first := p.nextLocalRetryAt.Sub(before)
	if first < time.Duration(float64(initialLocalBackoff)*0.75) || first > time.Duration(float64(initialLocalBackoff)*1.25)+time.Millisecond {
		t.Fatalf("first backoff = %s, want about %s ±25%%", first, initialLocalBackoff)
	}

	for i := 0; i < 8; i++ {
		p.noteLocalAttempt(ErrPublisherTimeout)
	}
	if p.localRetryBackoff > maxLocalBackoff {
		t.Fatalf("backoff = %s, cap = %s", p.localRetryBackoff, maxLocalBackoff)
	}

	before = time.Now()
	p.noteLocalAttempt(errors.New("Forbidden: Number of Mobile App connections above limit, declining connection"))
	limited := p.nextLocalRetryAt.Sub(before)
	if limited < time.Duration(float64(maxConnectionBackoff)*0.75) || limited > time.Duration(float64(maxConnectionBackoff)*1.25)+time.Millisecond {
		t.Fatalf("connection-limit backoff = %s, want about %s ±25%%", limited, maxConnectionBackoff)
	}

	p.noteLocalAttempt(nil)
	assert.Equal(t, time.Duration(0), p.localRetryBackoff)
}
