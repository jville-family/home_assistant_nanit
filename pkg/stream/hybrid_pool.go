package stream

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/rtmpserver"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
)

// Source IDs for hybrid pool
// Local uses rtmpserver.SourceIDLocal (1) to match what the RTMP handler uses
// when the camera publishes directly to the server
const (
	HybridSourceLocal  = 1  // Must match rtmpserver.SourceIDLocal
	HybridSourceRemote = 21 // Remote cloud relay (unique ID for remote source)
)

// streamingCommander sends PUT_STREAMING on the existing camera websocket.
// It does not open another websocket session.
type streamingCommander interface {
	SendRequest(reqType client.RequestType, requestData *client.Request) func(time.Duration) (*client.Response, error)
}

// Recovery configuration.
// Local re-puts stay on the existing websocket and back off so a dead publish
// cannot thrash the camera's mobile connection slots (~2–3).
const (
	minLocalRetryInterval   = 20 * time.Second
	initialLocalBackoff     = 20 * time.Second
	maxLocalBackoff         = 90 * time.Second
	maxConnectionBackoff    = 3 * time.Minute
	localRetryJitter        = 0.25
	healthCheckInterval     = 5 * time.Second
	defaultLocalPublishWait = 15 * time.Second
)

// HybridStreamPool manages a local stream as primary with remote relay as hot standby.
// This provides true redundancy because local and remote use different network paths.
// When local stalls, it switches to remote using keyframe-aligned switching.
type HybridStreamPool struct {
	babyUID      string
	localURL     string // RTMP URL for local stream
	conn         streamingCommander
	stateManager *baby.StateManager
	rtmpServer   *rtmpserver.RTMPServer
	health       *LocalHealthRegistry

	// Remote relay configuration
	authToken     string
	tokenProvider TokenProvider   // Provides fresh tokens on reconnect
	onAuthFailed  func(err error) // Called when auth fails completely
	remoteRelay   RemoteRelay     // Interface, actual type is *RTMPRelay

	mu           sync.RWMutex
	started      bool
	activeSource int  // HybridSourceLocal or HybridSourceRemote
	localActive  bool // Whether local publisher is connected
	remoteActive bool // Whether remote relay is connected
	ctx          context.Context
	cancel       context.CancelFunc

	// Stall detection config
	stallThreshold time.Duration

	// Recovery tracking
	lastLocalStall       time.Time
	localStallCount      int
	lastLocalRetry       time.Time
	nextLocalRetryAt     time.Time
	localRetryInFlight   bool
	localRetryBackoff    time.Duration
	localPublishWait     time.Duration
	absentSince          time.Time
	publisherSince       time.Time
	localRTMPHealthy     bool
	localReportedStarved bool

	// localStats overrides publisher count and last local media time in tests.
	localStats func() (publisherCount int, lastMedia time.Time)
}

// HybridStreamPoolConfig configuration for HybridStreamPool
type HybridStreamPoolConfig struct {
	BabyUID        string
	LocalURL       string        // RTMP URL for local stream (without slot suffix)
	AuthToken      string        // Initial auth token for remote relay
	TokenProvider  TokenProvider // Optional: provides fresh tokens on reconnect (recommended)
	Conn           *client.WebsocketConnection
	StateManager   *baby.StateManager
	RTMPServer     *rtmpserver.RTMPServer
	StallThreshold time.Duration
	OnAuthFailed   func(err error) // Optional: called when auth fails completely
	// Health receives local RTMP health updates. Nil skips HTTP reporting.
	Health *LocalHealthRegistry
}

// NewHybridStreamPool creates a new hybrid stream pool
func NewHybridStreamPool(config HybridStreamPoolConfig) *HybridStreamPool {
	if config.StallThreshold == 0 {
		config.StallThreshold = 3 * time.Second // Default 3 second stall threshold
	}

	return &HybridStreamPool{
		babyUID:          config.BabyUID,
		localURL:         config.LocalURL,
		authToken:        config.AuthToken,
		tokenProvider:    config.TokenProvider,
		onAuthFailed:     config.OnAuthFailed,
		conn:             config.Conn,
		stateManager:     config.StateManager,
		rtmpServer:       config.RTMPServer,
		health:           config.Health,
		stallThreshold:   config.StallThreshold,
		localPublishWait: defaultLocalPublishWait,
		activeSource:     HybridSourceLocal, // Start with local as primary
	}
}

// RequestLocalStream starts both local and remote streams (local as active, remote as standby)
func (p *HybridStreamPool) RequestLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		// Runtime recovery is the health watchdog and the publisher-drop callback.
		// Calling RequestLocalStream again must not open another session.
		p.mu.Unlock()
		return nil
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true
	p.activeSource = HybridSourceLocal
	if p.absentSince.IsZero() {
		p.absentSince = time.Now()
	}
	p.mu.Unlock()

	if p.rtmpServer != nil {
		p.rtmpServer.AddLocalPublisherListener(p.onLocalPublisherCount)
	}

	// Publish initial stream source state
	p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))

	// Configure stall detection on the broadcaster
	p.configureStallDetection()

	// Start remote relay as hot standby (relay.Start returns immediately)
	p.startRemoteRelay()

	// Request local stream (primary)
	if err := p.requestLocalStream(); err != nil {
		log.Warn().Err(err).Str("baby_uid", p.babyUID).Msg("Failed to start local stream, falling back to remote")

		// Actually switch to remote since local failed
		p.mu.Lock()
		p.activeSource = HybridSourceRemote
		p.mu.Unlock()

		// Update broadcaster to accept remote packets
		if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
			broadcaster.SetActiveSource(HybridSourceRemote)
			log.Info().Str("baby_uid", p.babyUID).Msg("Switched active source to remote after local failure")
		}

		// Publish stream source change to remote
		p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Remote))
	}

	// Start health watchdog to monitor and recover streams
	go p.runHealthWatchdog(p.ctx)

	return nil
}

// configureStallDetection sets up the stall callback on the broadcaster
func (p *HybridStreamPool) configureStallDetection() {
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster == nil {
		// Create the broadcaster if it doesn't exist
		p.rtmpServer.GetOrCreateBroadcaster(p.babyUID)
		broadcaster = p.rtmpServer.GetBroadcaster(p.babyUID)
	}

	if broadcaster == nil {
		log.Warn().Str("baby_uid", p.babyUID).Msg("No broadcaster found for stall detection")
		return
	}

	broadcaster.SetStallConfig(&rtmpserver.StallConfig{
		Threshold: p.stallThreshold,
		Callback:  p.onStallDetected,
	})

	// Set initial active source to local
	broadcaster.SetActiveSource(HybridSourceLocal)

	log.Info().
		Str("baby_uid", p.babyUID).
		Dur("threshold", p.stallThreshold).
		Msg("Configured stall detection for hybrid stream pool (local primary, remote standby)")
}

// onStallDetected is called when the broadcaster detects a stall on the active source
func (p *HybridStreamPool) onStallDetected(sourceID int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}

	// Ignore stalls from non-active source
	if sourceID != p.activeSource {
		log.Debug().
			Str("baby_uid", p.babyUID).
			Int("stalled_source", sourceID).
			Int("active_source", p.activeSource).
			Msg("Stall detected on non-active source, ignoring")
		return
	}

	now := time.Now()

	if sourceID == HybridSourceLocal {
		// Local stalled - switch to remote
		p.lastLocalStall = now
		p.localStallCount++

		log.Warn().
			Str("baby_uid", p.babyUID).
			Int("stall_count", p.localStallCount).
			Bool("remote_active", p.remoteActive).
			Msg("Local stream stalled, attempting switch to remote")

		if p.remoteActive {
			p.activeSource = HybridSourceRemote

			// Use keyframe-aligned switching for graceful transition
			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceRemote)
			}

			// Publish stream source change
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Remote))

			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Requested switch to remote stream (waiting for keyframe)")
		} else {
			log.Warn().
				Str("baby_uid", p.babyUID).
				Msg("Local stream stalled and remote standby is not available, re-requesting local publish")

			// Keep remote as standby only. Local re-put is what brings the camera back.
			go p.startRemoteRelay()
		}

		// Remote availability must not gate the local re-put. A stall callback
		// fires once per gap, so the health watchdog retries if this attempt fails.
		go p.tryRequestLocalStream()
	} else if sourceID == HybridSourceRemote {
		// Remote stalled - switch back to local only when local media is actually flowing.
		count, lastMedia := p.readLocalStats()
		localHealthy := (LocalRTMPSnapshot{
			PublisherCount: count,
			LastMediaAt:    lastMedia,
			PublisherSince: p.publisherSince,
			AbsentSince:    p.absentSince,
			Now:            now,
		}).Healthy()

		log.Warn().
			Str("baby_uid", p.babyUID).
			Bool("local_rtmp_healthy", localHealthy).
			Msg("Remote stream stalled, attempting switch to local")

		if localHealthy {
			p.activeSource = HybridSourceLocal

			// Use keyframe-aligned switching
			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceLocal)
			}

			// Publish stream source change
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))

			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Requested switch to local stream (waiting for keyframe)")
		} else {
			log.Warn().
				Str("baby_uid", p.babyUID).
				Msg("Remote stalled and local publish is not healthy, re-requesting local publish")

			go p.tryRequestLocalStream()
		}
	}
}

// startRemoteRelay starts the remote relay as hot standby
func (p *HybridStreamPool) startRemoteRelay() {
	p.mu.Lock()

	// Check if pool has been stopped
	if !p.started {
		p.mu.Unlock()
		log.Debug().Str("baby_uid", p.babyUID).Msg("Pool stopped, skipping remote relay start")
		return
	}

	if p.remoteRelay != nil {
		p.mu.Unlock()
		return // Already running
	}

	// Get the broadcaster's broadcast function for remote source
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster == nil {
		p.mu.Unlock()
		log.Error().Str("baby_uid", p.babyUID).Msg("No broadcaster for remote relay")
		return
	}

	broadcastFunc := broadcaster.GetBroadcastFunc(HybridSourceRemote)
	broadcasterAdapter := NewBroadcasterAdapter(broadcastFunc)

	relay := NewRemoteRelay(RemoteRelayConfig{
		BabyUID:              p.babyUID,
		AuthToken:            p.authToken,
		TokenProvider:        p.tokenProvider,
		PacketReceiver:       broadcasterAdapter,
		RTMPClient:           NewRealRTMPClient(),
		ReconnectEnabled:     true,
		MaxReconnectAttempts: 5, // After 5 consecutive failures, notify pool to try local
		OnConnected: func() {
			p.mu.Lock()
			p.remoteActive = true
			p.mu.Unlock()
			log.Info().Str("baby_uid", p.babyUID).Msg("Remote relay connected")
		},
		OnMaxRetriesExceeded: func(lastErr error) {
			p.onRemoteMaxRetriesExceeded(lastErr)
		},
		OnAuthFailed: func(err error) {
			log.Error().
				Err(err).
				Str("baby_uid", p.babyUID).
				Msg("Remote relay authentication failed - manual re-authentication may be required")
			if p.onAuthFailed != nil {
				p.onAuthFailed(err)
			}
		},
	})

	p.remoteRelay = relay
	ctx := p.ctx
	p.mu.Unlock()

	log.Info().Str("baby_uid", p.babyUID).Msg("Starting remote relay as hot standby")

	// Start the relay in background - it handles its own reconnection internally
	// Note: remoteActive is set via OnConnected callback when actually connected
	go func() {
		err := relay.Start(ctx)

		// Only log and mark inactive if it wasn't a normal context cancellation
		if err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Str("baby_uid", p.babyUID).Msg("Remote relay stopped unexpectedly")
		}

		p.mu.Lock()
		p.remoteActive = false
		p.mu.Unlock()
	}()
}

// onRemoteMaxRetriesExceeded is called when remote relay has failed too many times
func (p *HybridStreamPool) onRemoteMaxRetriesExceeded(lastErr error) {
	p.mu.Lock()
	activeSource := p.activeSource
	started := p.started
	p.remoteActive = false // Mark remote as unavailable
	p.mu.Unlock()

	if !started {
		return
	}

	snap := p.observeLocal(time.Now())
	log.Warn().
		Err(lastErr).
		Str("baby_uid", p.babyUID).
		Int("active_source", activeSource).
		Bool("local_rtmp_healthy", snap.Healthy()).
		Msg("Remote relay max retries exceeded, attempting fallback to local")

	if snap.Healthy() {
		if activeSource == HybridSourceRemote {
			p.preferLocal()
		}
		return
	}

	log.Info().Str("baby_uid", p.babyUID).Msg("Local publish is not healthy, requesting local stream")
	go p.tryRequestLocalStream()
}

// requestLocalStream requests the local stream from the camera on the existing websocket.
// Success requires a local publisher and media newer than this request. A socket that is
// still registered but silent does not count.
func (p *HybridStreamPool) requestLocalStream() error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	ownedFlight := false
	if !p.localRetryInFlight {
		p.localRetryInFlight = true
		ownedFlight = true
	}
	p.mu.Unlock()

	if ownedFlight {
		defer func() {
			p.mu.Lock()
			p.localRetryInFlight = false
			p.mu.Unlock()
		}()
	}

	log.Info().
		Str("baby_uid", p.babyUID).
		Str("url", p.localURL).
		Msg("Requesting local stream")

	requestedAt := time.Now()
	awaitResponse := p.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(p.localURL),
			Status:   client.Streaming_Status(client.Streaming_STARTED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})

	if _, err := awaitResponse(30 * time.Second); err != nil {
		log.Error().
			Err(err).
			Str("baby_uid", p.babyUID).
			Msg("Failed to request local stream")
		p.noteLocalAttempt(err)
		return err
	}

	log.Info().Str("baby_uid", p.babyUID).Msg("Local stream requested successfully")

	if err := p.waitForFreshLocalMedia(requestedAt, p.publisherWait()); err != nil {
		log.Warn().
			Err(err).
			Str("baby_uid", p.babyUID).
			Msg("Local publisher didn't deliver media")
		p.noteLocalAttempt(err)
		return err
	}

	p.mu.Lock()
	p.localActive = true
	p.mu.Unlock()

	log.Info().Str("baby_uid", p.babyUID).Msg("Local stream active")
	p.noteLocalAttempt(nil)
	p.preferLocal()

	count, lastMedia := p.readLocalStats()
	now := time.Now()
	p.publishHealth(LocalRTMPSnapshot{
		PublisherCount: count,
		LastMediaAt:    lastMedia,
		Now:            now,
	}, true)
	p.mu.Lock()
	alreadyHealthy := p.localRTMPHealthy
	p.localRTMPHealthy = true
	p.localReportedStarved = false
	p.mu.Unlock()
	if !alreadyHealthy {
		log.Info().
			Str("baby_uid", p.babyUID).
			Bool("local_rtmp_healthy", true).
			Int("publisher_count", count).
			Msg("Local RTMP healthy")
	}

	if p.stateManager != nil {
		p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))
	}

	return nil
}

func (p *HybridStreamPool) publisherWait() time.Duration {
	p.mu.RLock()
	wait := p.localPublishWait
	p.mu.RUnlock()
	if wait <= 0 {
		return defaultLocalPublishWait
	}
	return wait
}

// waitForFreshLocalMedia waits until a local publisher sends media newer than after.
func (p *HybridStreamPool) waitForFreshLocalMedia(after time.Time, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var ctxDone <-chan struct{}
	if p.ctx != nil {
		ctxDone = p.ctx.Done()
	}

	for {
		count, lastMedia := p.readLocalStats()
		if count > 0 && lastMedia.After(after) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return ErrPublisherTimeout
		}
		select {
		case <-ctxDone:
			if p.ctx != nil {
				return p.ctx.Err()
			}
		case <-ticker.C:
		}
	}
}

// tryRequestLocalStream attempts to request a local stream with rate limiting and backoff.
// This prevents spamming the camera when local publish is unavailable.
func (p *HybridStreamPool) tryRequestLocalStream() {
	p.mu.Lock()

	if !p.started {
		p.mu.Unlock()
		log.Debug().Str("baby_uid", p.babyUID).Msg("Pool stopped, skipping local retry")
		return
	}

	if p.localRetryInFlight {
		p.mu.Unlock()
		log.Debug().Str("baby_uid", p.babyUID).Msg("Local retry already in flight, skipping")
		return
	}

	now := time.Now()
	if !p.nextLocalRetryAt.IsZero() && now.Before(p.nextLocalRetryAt) {
		p.mu.Unlock()
		log.Debug().
			Str("baby_uid", p.babyUID).
			Time("next_retry", p.nextLocalRetryAt).
			Msg("Local retry in backoff, skipping")
		return
	}

	p.localRetryInFlight = true
	p.lastLocalRetry = now
	backoff := p.localRetryBackoff
	p.mu.Unlock()

	log.Info().
		Str("baby_uid", p.babyUID).
		Dur("backoff", backoff).
		Msg("Attempting local stream request")

	err := p.requestLocalStream()

	p.mu.Lock()
	p.localRetryInFlight = false
	next := p.nextLocalRetryAt
	p.mu.Unlock()

	if err != nil {
		log.Warn().
			Err(err).
			Str("baby_uid", p.babyUID).
			Time("next_retry", next).
			Msg("Local stream request failed, increased backoff")
		return
	}
	log.Info().Str("baby_uid", p.babyUID).Msg("Local stream request succeeded, reset backoff")
}

// noteLocalAttempt schedules the next local re-put. Failures back off with jitter.
// A mobile-connection limit uses a longer backoff so we do not thrash camera slots.
func (p *HybridStreamPool) noteLocalAttempt(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if err == nil {
		p.localRetryBackoff = 0
		p.nextLocalRetryAt = now.Add(applyJitter(minLocalRetryInterval, localRetryJitter))
		return
	}

	if IsMaxConnectionsError(err) {
		p.localRetryBackoff = maxConnectionBackoff
	} else if p.localRetryBackoff == 0 {
		p.localRetryBackoff = initialLocalBackoff
	} else {
		p.localRetryBackoff = time.Duration(float64(p.localRetryBackoff) * 1.5)
		if p.localRetryBackoff > maxLocalBackoff {
			p.localRetryBackoff = maxLocalBackoff
		}
	}
	p.nextLocalRetryAt = now.Add(applyJitter(p.localRetryBackoff, localRetryJitter))
}

// runHealthWatchdog periodically checks local RTMP health and re-requests local publish
// when the camera is not actually sending media. Remote standby does not satisfy this check.
func (p *HybridStreamPool) runHealthWatchdog(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			started := p.started
			p.mu.RUnlock()
			if !started {
				return
			}
			p.checkLocalHealth(time.Now())
		}
	}
}

// readLocalStats returns the local publisher count and last local media time.
func (p *HybridStreamPool) readLocalStats() (int, time.Time) {
	if p.localStats != nil {
		return p.localStats()
	}
	if p.rtmpServer == nil {
		return 0, time.Time{}
	}
	return p.rtmpServer.LocalPublisherCount(p.babyUID), p.rtmpServer.LastLocalMediaAt(p.babyUID)
}

// observeLocal updates publisher/absent clocks from the RTMP server and returns a snapshot.
// MQTT alive flags are not an input.
func (p *HybridStreamPool) observeLocal(now time.Time) LocalRTMPSnapshot {
	count, lastMedia := p.readLocalStats()

	p.mu.Lock()
	defer p.mu.Unlock()

	if count > 0 {
		if p.publisherSince.IsZero() {
			p.publisherSince = now
		}
		p.absentSince = time.Time{}
		p.localActive = true
	} else {
		p.publisherSince = time.Time{}
		if p.absentSince.IsZero() {
			p.absentSince = now
		}
		p.localActive = false
	}

	return LocalRTMPSnapshot{
		PublisherCount: count,
		LastMediaAt:    lastMedia,
		PublisherSince: p.publisherSince,
		AbsentSince:    p.absentSince,
		Now:            now,
	}
}

// checkLocalHealth re-requests local publish when it is starved, and prefers local
// again once media is flowing. Remote standby does not count as local health.
func (p *HybridStreamPool) checkLocalHealth(now time.Time) {
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	if !started {
		return
	}

	snap := p.observeLocal(now)
	healthy := snap.Healthy()
	starved := snap.Starved()

	p.mu.Lock()
	prevHealthy := p.localRTMPHealthy
	prevStarved := p.localReportedStarved
	active := p.activeSource
	p.localRTMPHealthy = healthy
	p.localReportedStarved = starved
	p.mu.Unlock()

	p.publishHealth(snap, healthy)

	if healthy && !prevHealthy {
		age := time.Duration(0)
		if !snap.LastMediaAt.IsZero() {
			age = snap.Now.Sub(snap.LastMediaAt)
			if age < 0 {
				age = 0
			}
		}
		log.Info().
			Str("baby_uid", p.babyUID).
			Bool("local_rtmp_healthy", true).
			Int("publisher_count", snap.PublisherCount).
			Dur("local_media_age", age).
			Msg("Local RTMP healthy")
	}
	if starved && !prevStarved {
		absent := time.Duration(0)
		if !snap.AbsentSince.IsZero() {
			absent = snap.Now.Sub(snap.AbsentSince)
		}
		log.Warn().
			Str("baby_uid", p.babyUID).
			Bool("local_rtmp_healthy", false).
			Int("publisher_count", snap.PublisherCount).
			Dur("absent_for", absent).
			Msg("Local RTMP starved")
	}

	if starved {
		go p.tryRequestLocalStream()
	}
	if healthy && active == HybridSourceRemote {
		p.preferLocal()
	}
}

func (p *HybridStreamPool) publishHealth(snap LocalRTMPSnapshot, healthy bool) {
	if p.health == nil {
		return
	}
	report := LocalHealthReport{
		BabyUID:        p.babyUID,
		Healthy:        healthy,
		PublisherCount: snap.PublisherCount,
	}
	if !snap.LastMediaAt.IsZero() {
		report.LastMediaAt = snap.LastMediaAt
		age := snap.Now.Sub(snap.LastMediaAt)
		if age < 0 {
			age = 0
		}
		report.LocalMediaAgeSec = age.Seconds()
	}
	p.health.Set(report)
}

// preferLocal makes the local camera publish the forwarded source again.
func (p *HybridStreamPool) preferLocal() {
	p.mu.Lock()
	if !p.started || p.activeSource == HybridSourceLocal {
		p.mu.Unlock()
		return
	}
	p.activeSource = HybridSourceLocal
	p.mu.Unlock()

	if p.rtmpServer != nil {
		if p.rtmpServer.GetBroadcaster(p.babyUID) == nil {
			p.rtmpServer.GetOrCreateBroadcaster(p.babyUID)
		}
		if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
			broadcaster.RequestSourceSwitch(HybridSourceLocal)
		}
	}
	if p.stateManager != nil {
		p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))
	}
	log.Info().
		Str("baby_uid", p.babyUID).
		Msg("Local media restored, preferring local publish over remote standby")
}

// onLocalPublisherCount is invoked by the RTMP server on the connection goroutine.
// A drop to zero publishers re-issues PUT_STREAMING without waiting for MQTT flags.
func (p *HybridStreamPool) onLocalPublisherCount(babyUID string, count int) {
	if babyUID != p.babyUID || count > 0 {
		return
	}
	go p.notePublisherGone()
}

func (p *HybridStreamPool) notePublisherGone() {
	p.mu.Lock()
	p.localActive = false
	p.localRTMPHealthy = false
	if p.absentSince.IsZero() {
		p.absentSince = time.Now()
	}
	started := p.started
	p.mu.Unlock()

	if !started {
		return
	}

	log.Info().
		Str("baby_uid", p.babyUID).
		Msg("Local publisher gone, re-requesting local stream")
	p.tryRequestLocalStream()
}

// WaitForPublisher implements LocalStreamer interface
func (p *HybridStreamPool) WaitForPublisher(ctx context.Context, timeout time.Duration) error {
	return p.rtmpServer.WaitForPublisher(p.babyUID, timeout)
}

// StopLocalStream stops both local and remote streams
func (p *HybridStreamPool) StopLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = false
	if p.cancel != nil {
		p.cancel()
	}
	remoteRelay := p.remoteRelay
	p.remoteRelay = nil
	p.mu.Unlock()

	// Stop stall detection
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster != nil {
		broadcaster.SetStallConfig(nil)
	}

	// Stop remote relay
	if remoteRelay != nil {
		remoteRelay.Stop()
	}

	// Stop local stream
	p.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(p.localURL),
			Status:   client.Streaming_Status(client.Streaming_STOPPED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})

	log.Info().Str("baby_uid", p.babyUID).Msg("Hybrid stream pool stopped")
	return nil
}

// OnPublisherConnected is called when a local publisher connects
func (p *HybridStreamPool) OnPublisherConnected(url string) {
	if url != p.localURL {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.localActive = true
	log.Debug().
		Str("baby_uid", p.babyUID).
		Msg("Local publisher connected")
}

// OnPublisherDisconnected is called when a local publisher disconnects
func (p *HybridStreamPool) OnPublisherDisconnected(url string) {
	if url != p.localURL {
		return
	}

	p.mu.Lock()
	wasActive := p.localActive
	p.localActive = false
	activeSource := p.activeSource
	started := p.started
	p.mu.Unlock()

	log.Info().
		Str("baby_uid", p.babyUID).
		Bool("was_active", wasActive).
		Msg("Local publisher disconnected")

	if !started {
		return
	}

	// If we were using local, switch to remote
	if activeSource == HybridSourceLocal {
		p.mu.Lock()
		if p.remoteActive {
			p.activeSource = HybridSourceRemote
			p.mu.Unlock()

			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceRemote)
			}

			// Publish stream source change to remote
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Remote))

			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Switched to remote after local disconnect")
		} else {
			p.mu.Unlock()
			log.Warn().
				Str("baby_uid", p.babyUID).
				Msg("Local disconnected but remote not available")
		}

		// Try to restart local (rate-limited)
		go p.tryRequestLocalStream()
	}
}

// GetActiveSource returns the currently active source
func (p *HybridStreamPool) GetActiveSource() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeSource
}

// IsLocalRTMPHealthy reports whether local RTMP has a publisher with recent media.
// This is independent of MQTT is_stream_alive.
func (p *HybridStreamPool) IsLocalRTMPHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.localRTMPHealthy
}

// IsLocalActive returns whether a local publisher socket is registered.
// It does not mean media is flowing. Use IsLocalRTMPHealthy for that.
func (p *HybridStreamPool) IsLocalActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.localActive
}

// IsRemoteActive returns whether remote relay is connected
func (p *HybridStreamPool) IsRemoteActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.remoteActive
}
