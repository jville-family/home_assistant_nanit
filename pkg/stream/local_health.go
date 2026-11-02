package stream

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	// LocalMediaFresh is how recently a local publisher must have sent media
	// before local RTMP counts as healthy for viewers and /health/local-rtmp.
	LocalMediaFresh = 5 * time.Second

	// LocalStarveAfter is how long local RTMP can be without a publisher, or
	// without local media, before the restreamer re-issues PUT_STREAMING.
	LocalStarveAfter = 15 * time.Second

	// LocalNoMediaGrace is how long a newly connected publisher may be silent
	// before that silence counts as starvation.
	LocalNoMediaGrace = 15 * time.Second
)

// LocalRTMPSnapshot is the local-publish state used for recovery and health.
// It is derived from the RTMP server (publisher count and local media time),
// not from MQTT is_stream_alive or stream_source.
type LocalRTMPSnapshot struct {
	PublisherCount int
	LastMediaAt    time.Time
	PublisherSince time.Time
	AbsentSince    time.Time
	Now            time.Time
}

// Healthy reports whether a local publisher is connected and has sent media recently.
func (s LocalRTMPSnapshot) Healthy() bool {
	if s.PublisherCount <= 0 || s.LastMediaAt.IsZero() {
		return false
	}
	return s.Now.Sub(s.LastMediaAt) <= LocalMediaFresh
}

// Starved reports whether local publish has been missing long enough to re-request it.
// A publisher that just connected is given LocalNoMediaGrace before silence counts.
func (s LocalRTMPSnapshot) Starved() bool {
	if s.PublisherCount <= 0 {
		if s.AbsentSince.IsZero() {
			return false
		}
		return s.Now.Sub(s.AbsentSince) >= LocalStarveAfter
	}
	if s.LastMediaAt.IsZero() {
		if s.PublisherSince.IsZero() {
			return false
		}
		return s.Now.Sub(s.PublisherSince) >= LocalNoMediaGrace
	}
	return s.Now.Sub(s.LastMediaAt) >= LocalStarveAfter
}

// applyJitter returns d adjusted by up to ±factor (factor 0.25 → ±25%).
func applyJitter(d time.Duration, factor float64) time.Duration {
	if d <= 0 || factor <= 0 {
		return d
	}
	min := float64(d) * (1 - factor)
	span := float64(d)*(1+factor) - min
	return time.Duration(min + rand.Float64()*span)
}

// LocalHealthReport is one baby's local RTMP health.
// Healthy means a publisher is connected and local media arrived within LocalMediaFresh.
type LocalHealthReport struct {
	BabyUID          string    `json:"baby_uid"`
	Healthy          bool      `json:"healthy"`
	PublisherCount   int       `json:"publisher_count"`
	LastMediaAt      time.Time `json:"last_media_at,omitempty"`
	LocalMediaAgeSec float64   `json:"local_media_age_sec,omitempty"`
}

// LocalHealthRegistry stores the latest local RTMP health for each baby.
type LocalHealthRegistry struct {
	mu     sync.RWMutex
	babies map[string]LocalHealthReport
}

// NewLocalHealthRegistry creates an empty registry.
func NewLocalHealthRegistry() *LocalHealthRegistry {
	return &LocalHealthRegistry{babies: make(map[string]LocalHealthReport)}
}

// Set records the latest report for a baby.
func (r *LocalHealthRegistry) Set(report LocalHealthReport) {
	if r == nil || report.BabyUID == "" {
		return
	}
	r.mu.Lock()
	r.babies[report.BabyUID] = report
	r.mu.Unlock()
}

// Snapshot returns whether every registered baby is healthy, and the reports.
// An empty registry is not healthy.
func (r *LocalHealthRegistry) Snapshot() (bool, []LocalHealthReport) {
	if r == nil {
		return false, nil
	}
	r.mu.RLock()
	reports := make([]LocalHealthReport, 0, len(r.babies))
	for _, report := range r.babies {
		reports = append(reports, report)
	}
	r.mu.RUnlock()

	sort.Slice(reports, func(i, j int) bool {
		return reports[i].BabyUID < reports[j].BabyUID
	})
	if len(reports) == 0 {
		return false, reports
	}
	for _, report := range reports {
		if !report.Healthy {
			return false, reports
		}
	}
	return true, reports
}

// ServeHTTP handles GET /health/local-rtmp.
// 200 means every baby has a local publisher with recent media. 503 otherwise.
func (r *LocalHealthRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	healthy, reports := r.Snapshot()
	status := http.StatusServiceUnavailable
	if healthy {
		status = http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if req.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(struct {
		Healthy bool                `json:"healthy"`
		Babies  []LocalHealthReport `json:"babies"`
	}{
		Healthy: healthy,
		Babies:  reports,
	})
}
