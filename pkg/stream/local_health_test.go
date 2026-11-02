package stream

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalRTMPSnapshotHealthyRequiresPublisherAndRecentMedia(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name    string
		snap    LocalRTMPSnapshot
		healthy bool
		starved bool
	}{
		{
			name: "fresh media",
			snap: LocalRTMPSnapshot{
				PublisherCount: 1,
				LastMediaAt:    now.Add(-time.Second),
				PublisherSince: now.Add(-time.Minute),
				Now:            now,
			},
			healthy: true,
		},
		{
			name: "media older than freshness window",
			snap: LocalRTMPSnapshot{
				PublisherCount: 1,
				LastMediaAt:    now.Add(-LocalMediaFresh - time.Second),
				PublisherSince: now.Add(-time.Minute),
				Now:            now,
			},
		},
		{
			name: "no publisher",
			snap: LocalRTMPSnapshot{
				PublisherCount: 0,
				AbsentSince:    now,
				Now:            now,
			},
		},
		{
			name: "publisher missing long enough",
			snap: LocalRTMPSnapshot{
				PublisherCount: 0,
				AbsentSince:    now.Add(-LocalStarveAfter - time.Second),
				Now:            now,
			},
			starved: true,
		},
		{
			name: "publisher present but silent past grace",
			snap: LocalRTMPSnapshot{
				PublisherCount: 1,
				PublisherSince: now.Add(-LocalNoMediaGrace - time.Second),
				Now:            now,
			},
			starved: true,
		},
		{
			name: "new publisher still within grace",
			snap: LocalRTMPSnapshot{
				PublisherCount: 1,
				PublisherSince: now,
				Now:            now,
			},
		},
		{
			name: "stale media past starve threshold",
			snap: LocalRTMPSnapshot{
				PublisherCount: 1,
				LastMediaAt:    now.Add(-LocalStarveAfter - time.Second),
				PublisherSince: now.Add(-time.Minute),
				Now:            now,
			},
			starved: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.healthy, tc.snap.Healthy())
			assert.Equal(t, tc.starved, tc.snap.Starved())
		})
	}
}

func TestApplyJitterStaysWithinFactor(t *testing.T) {
	const base = 20 * time.Second
	for i := 0; i < 100; i++ {
		got := applyJitter(base, 0.25)
		if got < time.Duration(float64(base)*0.75) || got > time.Duration(float64(base)*1.25) {
			t.Fatalf("jitter %s outside ±25%% of %s", got, base)
		}
	}
	assert.Equal(t, base, applyJitter(base, 0))
}

func TestLocalHealthHandler(t *testing.T) {
	reg := NewLocalHealthRegistry()

	rec := httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/local-rtmp", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	reg.Set(LocalHealthReport{BabyUID: "b", Healthy: false, PublisherCount: 0})
	rec = httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/local-rtmp", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body struct {
		Healthy bool                `json:"healthy"`
		Babies  []LocalHealthReport `json:"babies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.False(t, body.Healthy)
	require.Len(t, body.Babies, 1)
	assert.Equal(t, "b", body.Babies[0].BabyUID)

	reg.Set(LocalHealthReport{
		BabyUID:          "b",
		Healthy:          true,
		PublisherCount:   1,
		LastMediaAt:      time.Now(),
		LocalMediaAgeSec: 0.2,
	})
	rec = httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/local-rtmp", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.True(t, body.Healthy)

	rec = httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/health/local-rtmp", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	rec = httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/health/local-rtmp", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	payload, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Empty(t, payload)
}
