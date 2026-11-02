package rtmpserver

import (
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
)

// ============================================================
// TESTS FOR RTMP SERVER
// These tests verify the RTMP server functionality with
// PersistentBroadcaster integration for stream failover.
// ============================================================

func TestRtmpURLRegex(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		shouldMatch bool
		expectedUID string
	}{
		{
			name:        "valid path with alphanumeric UID",
			path:        "/local/abc123",
			shouldMatch: true,
			expectedUID: "abc123",
		},
		{
			name:        "valid path with UUID-like UID",
			path:        "/local/a1b2c3d4-e5f6-7890-abcd-ef1234567890",
			shouldMatch: true,
			expectedUID: "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		},
		{
			name:        "valid path with slot suffix",
			path:        "/local/baby_123",
			shouldMatch: true,
			expectedUID: "baby", // "baby" is the UID, "_123" is the slot suffix
		},
		{
			name:        "valid path with hyphen",
			path:        "/local/baby-123",
			shouldMatch: true,
			expectedUID: "baby-123",
		},
		{
			name:        "invalid path - no baby UID",
			path:        "/local/",
			shouldMatch: false,
			expectedUID: "",
		},
		{
			name:        "invalid path - wrong prefix",
			path:        "/remote/abc123",
			shouldMatch: false,
			expectedUID: "",
		},
		{
			name:        "invalid path - root only",
			path:        "/",
			shouldMatch: false,
			expectedUID: "",
		},
		{
			name:        "invalid path - extra segments",
			path:        "/local/abc123/extra",
			shouldMatch: false,
			expectedUID: "",
		},
		{
			name:        "invalid path - uppercase not allowed",
			path:        "/local/ABC123",
			shouldMatch: false,
			expectedUID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			submatch := rtmpURLRX.FindStringSubmatch(tt.path)

			if tt.shouldMatch {
				// Regex has 4 groups: full match, type (local/live), babyUID, optional slot
				assert.GreaterOrEqual(t, len(submatch), 3, "Should have at least 3 groups")
				assert.Equal(t, tt.expectedUID, submatch[2], "babyUID should be in group 2")
			} else {
				assert.True(t, len(submatch) == 0 || len(submatch) < 3)
			}
		})
	}
}

func TestNewRtmpHandler(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	assert.NotNil(t, handler)
	assert.NotNil(t, handler.broadcastersByUID)
	assert.Same(t, stateManager, handler.babyStateManager)
}

func TestRtmpHandlerRegisterLocalPublisher(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	// Register local publisher (babyUID, slotSuffix, streamKey)
	broadcastFunc := handler.registerLocalPublisher("baby123", "", "baby123")
	assert.NotNil(t, broadcastFunc)

	// Verify broadcaster is registered
	handler.broadcastersMu.RLock()
	pb, exists := handler.broadcastersByUID["baby123"]
	handler.broadcastersMu.RUnlock()

	assert.True(t, exists)
	assert.NotNil(t, pb)
	assert.Equal(t, "baby123", pb.BabyUID())

	// Verify local publisher is marked active
	assert.True(t, handler.IsLocalPublisherActive("baby123"))
}

func TestRtmpHandlerRegisterLocalPublisherReusesExistingBroadcaster(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	// Register first publisher
	handler.registerLocalPublisher("baby123", "", "baby123")

	// Get the broadcaster
	handler.broadcastersMu.RLock()
	pb1 := handler.broadcastersByUID["baby123"]
	handler.broadcastersMu.RUnlock()

	// Unregister
	handler.unregisterLocalPublisher("baby123", "baby123")

	// Register again - should reuse the same PersistentBroadcaster
	handler.registerLocalPublisher("baby123", "", "baby123")

	handler.broadcastersMu.RLock()
	pb2 := handler.broadcastersByUID["baby123"]
	handler.broadcastersMu.RUnlock()

	// Same broadcaster should be reused
	assert.Same(t, pb1, pb2)
}

func TestRtmpHandlerGetNewSubscriber(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	// Try to get subscriber without broadcaster - should return nil
	sub := handler.getNewSubscriber("baby123")
	assert.Nil(t, sub)

	// Register local publisher (creates broadcaster)
	handler.registerLocalPublisher("baby123", "", "baby123")

	// Now subscriber should work
	sub = handler.getNewSubscriber("baby123")
	assert.NotNil(t, sub)
}

func TestRtmpHandlerUnregisterLocalPublisher(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	// Register publisher
	handler.registerLocalPublisher("baby123", "", "baby123")

	assert.True(t, handler.IsLocalPublisherActive("baby123"))

	// Unregister it
	handler.unregisterLocalPublisher("baby123", "baby123")

	// Should be marked inactive
	assert.False(t, handler.IsLocalPublisherActive("baby123"))

	// But broadcaster should still exist (for failover to remote)
	handler.broadcastersMu.RLock()
	_, exists := handler.broadcastersByUID["baby123"]
	handler.broadcastersMu.RUnlock()

	assert.True(t, exists, "Broadcaster should persist after local publisher disconnects")
}

func TestRtmpHandlerMultipleBabies(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	// Register publishers for different babies
	handler.registerLocalPublisher("baby1", "", "baby1")
	handler.registerLocalPublisher("baby2", "", "baby2")

	// Both should be registered
	handler.broadcastersMu.RLock()
	pb1 := handler.broadcastersByUID["baby1"]
	pb2 := handler.broadcastersByUID["baby2"]
	handler.broadcastersMu.RUnlock()

	assert.NotNil(t, pb1)
	assert.NotNil(t, pb2)
	assert.NotSame(t, pb1, pb2)
}

func TestRtmpHandlerLocalPublisherBroadcastsWithCorrectSourceID(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	// Register local publisher
	broadcastFunc := handler.registerLocalPublisher("baby123", "", "baby123")

	// Subscribe to receive packets
	sub := handler.getNewSubscriber("baby123")
	assert.NotNil(t, sub)

	// Broadcast a packet
	broadcastFunc(av.Packet{
		Type: 1,
		Data: []byte("test"),
	})

	// Should receive the packet
	select {
	case pkt := <-sub.Packets():
		assert.Equal(t, int(1), pkt.Type)
	default:
		t.Fatal("Should have received packet")
	}
}

func TestRTMPServerGetOrCreateBroadcaster(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	// Get broadcaster (creates one)
	broadcastFunc := server.GetOrCreateBroadcaster("baby123")
	assert.NotNil(t, broadcastFunc)

	// Get again - should return function for same broadcaster
	broadcastFunc2 := server.GetOrCreateBroadcaster("baby123")
	assert.NotNil(t, broadcastFunc2)

	// Verify only one broadcaster exists
	server.handler.broadcastersMu.RLock()
	count := len(server.handler.broadcastersByUID)
	server.handler.broadcastersMu.RUnlock()

	assert.Equal(t, 1, count)
}

func TestRTMPServerRemoveBroadcaster(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	// Create broadcaster
	server.GetOrCreateBroadcaster("baby123")

	// Subscribe
	sub := server.handler.getNewSubscriber("baby123")
	assert.NotNil(t, sub)

	// Remove broadcaster
	server.RemoveBroadcaster("baby123")

	// Broadcaster should be removed
	server.handler.broadcastersMu.RLock()
	_, exists := server.handler.broadcastersByUID["baby123"]
	server.handler.broadcastersMu.RUnlock()

	assert.False(t, exists)

	// Subscription channel should be closed
	_, open := <-sub.Packets()
	assert.False(t, open, "Subscription channel should be closed")
}

// --- WaitForPublisher Race Condition Tests ---

func TestWaitForPublisher_ReturnsImmediatelyIfPublisherAlreadyActive(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	// Register local publisher BEFORE calling WaitForPublisher
	server.handler.registerLocalPublisher("baby123", "", "baby123")

	// WaitForPublisher should return immediately without waiting
	start := time.Now()
	err := server.WaitForPublisher("baby123", 5*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, elapsed < 100*time.Millisecond, "Should return immediately, not wait for timeout (elapsed: %v)", elapsed)
}

func TestWaitForPublisher_ReturnsImmediatelyIfPublisherConnectsDuringLockAcquisition(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	// Simulate race condition: hold the lock while publisher connects
	server.handler.publisherWaitMu.Lock()

	// Publisher connects while we hold the lock
	go func() {
		time.Sleep(10 * time.Millisecond)
		server.handler.registerLocalPublisher("baby123", "", "baby123")
		server.handler.publisherWaitMu.Unlock()
	}()

	// WaitForPublisher should detect the publisher after acquiring lock
	start := time.Now()
	err := server.WaitForPublisher("baby123", 5*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, elapsed < 500*time.Millisecond, "Should detect publisher quickly, not wait for timeout (elapsed: %v)", elapsed)
}

func TestWaitForPublisher_TimesOutIfNoPublisher(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	// No publisher, should timeout
	start := time.Now()
	err := server.WaitForPublisher("baby123", 100*time.Millisecond)
	elapsed := time.Since(start)

	assert.Equal(t, ErrPublisherTimeout, err)
	assert.True(t, elapsed >= 100*time.Millisecond, "Should wait for full timeout (elapsed: %v)", elapsed)
}

func TestWaitForPublisher_SucceedsWhenPublisherConnectsDuringWait(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	// Publisher connects during wait
	go func() {
		time.Sleep(50 * time.Millisecond)
		// Simulate handleConnection: register publisher and signal wait channel
		server.handler.registerLocalPublisher("baby123", "", "baby123")
		server.handler.publisherWaitMu.Lock()
		if ch, exists := server.handler.publisherWaitCh["baby123"]; exists {
			close(ch)
			delete(server.handler.publisherWaitCh, "baby123")
		}
		server.handler.publisherWaitMu.Unlock()
	}()

	start := time.Now()
	err := server.WaitForPublisher("baby123", 5*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, elapsed < 200*time.Millisecond, "Should return when publisher connects (elapsed: %v)", elapsed)
	assert.True(t, elapsed > 40*time.Millisecond, "Should wait until publisher connects (elapsed: %v)", elapsed)
}

func TestLocalPublisherListenerSeesConnectAndDrop(t *testing.T) {
	stateManager := baby.NewStateManager()
	server := NewRTMPServer(stateManager)

	var counts []int
	server.AddLocalPublisherListener(func(babyUID string, publisherCount int) {
		if babyUID == "baby123" {
			counts = append(counts, publisherCount)
		}
	})

	server.handler.registerLocalPublisher("baby123", "", "baby123")
	server.handler.unregisterLocalPublisher("baby123", "baby123")

	assert.Equal(t, []int{1, 0}, counts)
	assert.Equal(t, 0, server.LocalPublisherCount("baby123"))
}

func TestPublisherCountOneConnectionIsOne(t *testing.T) {
	stateManager := baby.NewStateManager()
	handler := newRtmpHandler(stateManager)

	handler.registerLocalPublisher("baby123", "", "baby123")
	assert.Equal(t, 1, handler.localPublishers["baby123"])

	remaining := handler.unregisterLocalPublisher("baby123", "baby123")
	assert.Equal(t, 0, remaining)
	assert.Equal(t, 0, handler.localPublishers["baby123"])

	handler.registerLocalPublisher("baby123", "_0", "baby123_0")
	handler.registerLocalPublisher("baby123", "_1", "baby123_1")
	assert.Equal(t, 2, handler.localPublishers["baby123"])
	assert.Equal(t, 1, handler.localPublishers["baby123_0"])

	remaining = handler.unregisterLocalPublisher("baby123", "baby123_0")
	assert.Equal(t, 1, remaining)
	assert.Equal(t, 0, handler.localPublishers["baby123_0"])
	assert.Equal(t, 1, handler.localPublishers["baby123"])
}
