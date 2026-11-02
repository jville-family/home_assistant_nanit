package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/message"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/notification"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/session"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

const (
	// MaxResponseBodySize limits response body to 10MB to prevent DoS
	MaxResponseBodySize = 10 * 1024 * 1024
)

var myClient = &http.Client{Timeout: 30 * time.Second}

var ErrExpiredRefreshToken = errors.New("refresh token has expired, relogin required")
var ErrTransientFailure = errors.New("transient HTTP failure")
var ErrAuthorizationFailed = errors.New("authorization failed")
var ErrUnexpectedStatusCode = errors.New("unexpected status code")

// ------------------------------------------

type authResponsePayload struct {
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type babiesResponsePayload struct {
	Babies []baby.Baby `json:"babies"`
}

type messagesResponsePayload struct {
	Messages []message.Message `json:"messages"`
}

type sleepEventsResponsePayload struct {
	Events []notification.SleepEvent `json:"events"`
}

type sleepStatsResponsePayload struct {
	Exists bool                    `json:"exists"`
	Latest notification.SleepStats `json:"latest"`
}

type lastEventResponsePayload struct {
	Event *notification.SleepEvent `json:"event"`
}

// UserInfo represents the current user's information
type UserInfo struct {
	UID       string `json:"uid"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

type userResponsePayload struct {
	User UserInfo `json:"user"`
}

// Camera represents camera details from the API
type Camera struct {
	UID            string `json:"uid"`
	SerialNumber   string `json:"serial_number"`
	Status         string `json:"status"`
	StatusTime     int64  `json:"status_time"`
	FirmwareStatus string `json:"firmware_status"`
}

// BabyWithCamera represents baby info with camera details
type BabyWithCamera struct {
	UID       string  `json:"uid"`
	Name      string  `json:"name"`
	CameraUID string  `json:"camera_uid"`
	Camera    *Camera `json:"camera"`
}

type babyWithCameraResponsePayload struct {
	Baby BabyWithCamera `json:"baby"`
}

// NotificationSetting represents a single notification setting
type NotificationSetting struct {
	Key     string `json:"key"`
	Enabled bool   `json:"enabled"`
}

type notificationSettingsResponsePayload struct {
	Settings []NotificationSetting `json:"notification_settings"`
}

// CVRInfo represents continuous video recording information
type CVRInfo struct {
	Enabled   bool   `json:"enabled"`
	Status    string `json:"status"`
	ExpiresAt int64  `json:"expires_at"`
}

type cvrResponsePayload struct {
	CVR CVRInfo `json:"cvr"`
}

// FeedItem represents an item in the activity feed
type FeedItem struct {
	UID       string  `json:"uid"`
	Type      string  `json:"type"`
	Time      float64 `json:"time"`
	Title     string  `json:"title"`
	Thumbnail string  `json:"thumbnail"`
}

type feedResponsePayload struct {
	Feed []FeedItem `json:"feed"`
}

// SummaryData represents summary information
type SummaryData struct {
	Date          string  `json:"date"`
	TotalSleep    int     `json:"total_sleep"`
	NightSleep    int     `json:"night_sleep"`
	DaySleep      int     `json:"day_sleep"`
	Awakenings    int     `json:"awakenings"`
	LongestStretch int    `json:"longest_stretch"`
}

type summaryResponsePayload struct {
	Summary SummaryData `json:"summary"`
}

// ------------------------------------------

// NanitClient - client context
type NanitClient struct {
	authMu       sync.Mutex // Protects authorization flow
	Email        string
	Password     string
	RefreshToken string
	SessionStore *session.Store
}

// MaybeAuthorize - Performs authorization if we don't have token or we assume it is expired
func (c *NanitClient) MaybeAuthorize(force bool) error {
	c.authMu.Lock()
	defer c.authMu.Unlock()

	if force || c.SessionStore.GetAuthToken() == "" || time.Since(c.SessionStore.Session.AuthTime) > AuthTokenTimelife {
		return c.authorizeInternal()
	}
	return nil
}

// Authorize - performs authorization attempt (thread-safe)
func (c *NanitClient) Authorize() error {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.authorizeInternal()
}

// authorizeInternal - performs authorization (must be called with authMu held)
func (c *NanitClient) authorizeInternal() error {
	if c.SessionStore.GetRefreshToken() == "" {
		c.SessionStore.Session.RefreshToken = c.RefreshToken
	}

	if c.SessionStore.GetRefreshToken() != "" {
		err := c.renewSessionInternal()
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrExpiredRefreshToken) {
			log.Error().Err(err).Msg("Error occurred while trying to refresh the session")
			return fmt.Errorf("session refresh failed: %w", err)
		}
		// Refresh token expired, fall through to login
	}

	return c.loginInternal()
}

// RenewSession renews an existing session using a valid refresh token (thread-safe)
func (c *NanitClient) RenewSession() error {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.renewSessionInternal()
}

// renewSessionInternal renews session (must be called with authMu held)
func (c *NanitClient) renewSessionInternal() error {
	requestBody, err := json.Marshal(map[string]string{
		"refresh_token": c.SessionStore.GetRefreshToken(),
	})
	if err != nil {
		return fmt.Errorf("unable to marshal auth body: %w", err)
	}

	r, err := myClient.Post("https://api.nanit.com/tokens/refresh", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("unable to renew session: %w", err)
	}
	defer r.Body.Close()

	if r.StatusCode == 404 {
		log.Warn().Msg("Server responded with code 404 - refresh token has expired")
		return ErrExpiredRefreshToken
	} else if r.StatusCode < 200 || r.StatusCode > 299 {
		return fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, r.StatusCode)
	}

	// Limit response body size
	limitedReader := io.LimitReader(r.Body, MaxResponseBodySize)
	authResponse := new(authResponsePayload)
	if err := json.NewDecoder(limitedReader).Decode(authResponse); err != nil {
		return fmt.Errorf("unable to decode response: %w", err)
	}

	log.Info().Str("token", utils.AnonymizeToken(authResponse.AccessToken, 4)).Msg("Authorized")
	c.SessionStore.UpdateAuth(authResponse.AccessToken, authResponse.RefreshToken)

	if err := c.SessionStore.Save(); err != nil {
		log.Warn().Err(err).Msg("Failed to save session after renewal")
	}

	return nil
}

// Login performs login with email/password (thread-safe)
func (c *NanitClient) Login() error {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.loginInternal()
}

// loginInternal performs login (must be called with authMu held)
func (c *NanitClient) loginInternal() error {
	// SECURITY: Don't log email address
	log.Info().Msg("Authorizing using user credentials")

	requestBody, err := json.Marshal(map[string]string{
		"email":    c.Email,
		"password": c.Password,
	})
	if err != nil {
		return fmt.Errorf("unable to marshal auth body: %w", err)
	}

	req, err := http.NewRequest("POST", "https://api.nanit.com/login", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("nanit-api-version", "1")

	r, err := myClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to fetch auth token: %w", err)
	}
	defer r.Body.Close()

	if r.StatusCode == 401 {
		return fmt.Errorf("%w: invalid credentials or 2FA required", ErrAuthorizationFailed)
	} else if r.StatusCode != 201 {
		return fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, r.StatusCode)
	}

	limitedReader := io.LimitReader(r.Body, MaxResponseBodySize)
	authResponse := new(authResponsePayload)
	if err := json.NewDecoder(limitedReader).Decode(authResponse); err != nil {
		return fmt.Errorf("unable to decode response: %w", err)
	}

	log.Info().Str("token", utils.AnonymizeToken(authResponse.AccessToken, 4)).Msg("Authorized")
	c.SessionStore.UpdateAuth(authResponse.AccessToken, authResponse.RefreshToken)

	if err := c.SessionStore.Save(); err != nil {
		log.Warn().Err(err).Msg("Failed to save session after login")
	}

	return nil
}

// FetchAuthorized - makes authorized http request
func (c *NanitClient) FetchAuthorized(req *http.Request, data interface{}) error {
	// Clone request body for potential retry (body is consumed on first attempt)
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return fmt.Errorf("unable to read request body: %w", err)
		}
	}

	for i := 0; i < 2; i++ {
		// Restore body for each attempt
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		token := c.SessionStore.GetAuthToken()
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)

			res, err := myClient.Do(req)
			if err != nil {
				return fmt.Errorf("HTTP request failed: %w", err)
			}
			defer res.Body.Close()

			if res.StatusCode != 401 {
				if res.StatusCode != 200 {
					return fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, res.StatusCode)
				}

				limitedReader := io.LimitReader(res.Body, MaxResponseBodySize)
				if err := json.NewDecoder(limitedReader).Decode(data); err != nil {
					return fmt.Errorf("unable to decode response: %w", err)
				}

				return nil
			}

			log.Info().Msg("Token might be expired. Will try to re-authenticate.")
		}

		if err := c.Authorize(); err != nil {
			return err
		}
	}

	return fmt.Errorf("%w: failed after 2 attempts", ErrAuthorizationFailed)
}

// TryFetchAuthorized - makes authorized http request, returns error on transient failures
func (c *NanitClient) TryFetchAuthorized(req *http.Request, data interface{}) error {
	// Clone request body for potential retry (body is consumed on first attempt)
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return fmt.Errorf("%w: unable to read request body: %v", ErrTransientFailure, err)
		}
	}

	for i := 0; i < 2; i++ {
		// Restore body for each attempt
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		token := c.SessionStore.GetAuthToken()
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)

			res, err := myClient.Do(req)
			if err != nil {
				log.Warn().Err(err).Msg("HTTP request failed (transient)")
				return fmt.Errorf("%w: %v", ErrTransientFailure, err)
			}
			defer res.Body.Close()

			if res.StatusCode != 401 {
				if res.StatusCode != 200 {
					log.Warn().Int("code", res.StatusCode).Msg("Server responded with unexpected status code (transient)")
					return fmt.Errorf("%w: status code %d", ErrTransientFailure, res.StatusCode)
				}

				limitedReader := io.LimitReader(res.Body, MaxResponseBodySize)
				if err := json.NewDecoder(limitedReader).Decode(data); err != nil {
					log.Warn().Err(err).Msg("Unable to decode response (transient)")
					return fmt.Errorf("%w: %v", ErrTransientFailure, err)
				}

				return nil
			}

			log.Info().Msg("Token might be expired. Will try to re-authenticate.")
		}

		if err := c.Authorize(); err != nil {
			return err
		}
	}

	return fmt.Errorf("%w: failed authorization after 2 attempts", ErrTransientFailure)
}

// FetchBabies - fetches baby list
func (c *NanitClient) FetchBabies() ([]baby.Baby, error) {
	log.Info().Msg("Fetching babies list")
	req, err := http.NewRequest("GET", "https://api.nanit.com/babies", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(babiesResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	c.SessionStore.Session.Babies = data.Babies
	if err := c.SessionStore.Save(); err != nil {
		log.Warn().Err(err).Msg("Failed to save session after fetching babies")
	}
	return data.Babies, nil
}

// FetchMessages - fetches message list
func (c *NanitClient) FetchMessages(babyUID string, limit int) ([]message.Message, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/messages?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(messagesResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Messages, nil
}

// TryFetchMessages - fetches message list, returns error on transient failures
func (c *NanitClient) TryFetchMessages(babyUID string, limit int) ([]message.Message, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/messages?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(messagesResponsePayload)
	if err := c.TryFetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Messages, nil
}

// EnsureBabies - fetches baby list if not fetched already
func (c *NanitClient) EnsureBabies() ([]baby.Baby, error) {
	if len(c.SessionStore.Session.Babies) == 0 {
		return c.FetchBabies()
	}
	return c.SessionStore.Session.Babies, nil
}

// FetchNewMessages - fetches newest messages, ignores already fetched or old messages
// Returns empty list on transient errors so polling can continue
func (c *NanitClient) FetchNewMessages(babyUID string, defaultMessageTimeout time.Duration) []message.Message {
	fetchedMessages, err := c.TryFetchMessages(babyUID, 10)
	newMessages := make([]message.Message, 0)

	if err != nil {
		log.Warn().Err(err).Str("baby_uid", babyUID).Msg("Failed to fetch messages, will retry")
		return newMessages
	}

	if len(fetchedMessages) == 0 {
		log.Debug().Msg("No messages fetched")
		return newMessages
	}

	// Log all message types for debugging (helps identify unknown event types)
	for _, msg := range fetchedMessages {
		log.Debug().
			Str("baby_uid", babyUID).
			Int("id", msg.Id).
			Str("type", msg.Type).
			Time("time", msg.Time.Time()).
			Msg("Fetched message from API")
	}

	sort.Slice(fetchedMessages, func(i, j int) bool {
		return fetchedMessages[i].Time.Time().After(fetchedMessages[j].Time.Time())
	})

	lastSeenMessageTime := c.SessionStore.Session.LastSeenMessageTime
	messageTimeoutTime := lastSeenMessageTime
	log.Debug().Msgf("Last seen message time was %s", lastSeenMessageTime)

	if lastSeenMessageTime.IsZero() {
		messageTimeoutTime = time.Now().UTC().Add(-defaultMessageTimeout)
	}

	if lastSeenMessageTime.Before(fetchedMessages[0].Time.Time()) {
		lastSeenMessageTime = fetchedMessages[0].Time.Time()
		c.SessionStore.Session.LastSeenMessageTime = lastSeenMessageTime
		if err := c.SessionStore.Save(); err != nil {
			log.Warn().Err(err).Msg("Failed to save session after updating message time")
		}
	}

	filteredMessages := message.FilterMessages(fetchedMessages, func(msg message.Message) bool {
		return msg.Time.Time().After(messageTimeoutTime)
	})

	log.Debug().Msgf("Found %d new messages", len(filteredMessages))

	return filteredMessages
}

// FetchSleepEvents - fetches sleep events from the /events endpoint
func (c *NanitClient) FetchSleepEvents(babyUID string, limit int) ([]notification.SleepEvent, error) {
	url := fmt.Sprintf("https://api.nanit.com/babies/%s/events?limit=%d", babyUID, limit)
	return c.fetchSleepEventsFromURL(url, limit)
}

// fetchSleepEventsFromURL - internal method for testing with custom URL
func (c *NanitClient) fetchSleepEventsFromURL(url string, limit int) ([]notification.SleepEvent, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(sleepEventsResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Events, nil
}

// TryFetchSleepEvents - fetches sleep events, returns error on transient failures
func (c *NanitClient) TryFetchSleepEvents(babyUID string, limit int) ([]notification.SleepEvent, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/events?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(sleepEventsResponsePayload)
	if err := c.TryFetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Events, nil
}

// FetchSleepStats - fetches sleep statistics from the /stats/latest endpoint
func (c *NanitClient) FetchSleepStats(babyUID string) (*notification.SleepStats, error) {
	url := fmt.Sprintf("https://api.nanit.com/babies/%s/stats/latest", babyUID)
	return c.fetchSleepStatsFromURL(url)
}

// fetchSleepStatsFromURL - internal method for testing with custom URL
func (c *NanitClient) fetchSleepStatsFromURL(url string) (*notification.SleepStats, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(sleepStatsResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	if !data.Exists {
		return nil, nil
	}

	return &data.Latest, nil
}

// TryFetchSleepStats - fetches sleep stats, returns error on transient failures
func (c *NanitClient) TryFetchSleepStats(babyUID string) (*notification.SleepStats, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/stats/latest", babyUID), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(sleepStatsResponsePayload)
	if err := c.TryFetchAuthorized(req, data); err != nil {
		return nil, err
	}

	if !data.Exists {
		return nil, nil
	}

	return &data.Latest, nil
}

// FetchLastEvent - fetches the last event for a baby (quick check)
func (c *NanitClient) FetchLastEvent(babyUID string) (*notification.SleepEvent, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/events/last", babyUID), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(lastEventResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Event, nil
}

// TryFetchLastEvent - fetches the last event, returns error on transient failures
func (c *NanitClient) TryFetchLastEvent(babyUID string) (*notification.SleepEvent, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/events/last", babyUID), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(lastEventResponsePayload)
	if err := c.TryFetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Event, nil
}

// FetchBabyWithCamera - fetches baby info with camera details
func (c *NanitClient) FetchBabyWithCamera(babyUID string) (*BabyWithCamera, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s?include=camera", babyUID), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(babyWithCameraResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return &data.Baby, nil
}

// FetchUser - fetches current user information
func (c *NanitClient) FetchUser() (*UserInfo, error) {
	req, err := http.NewRequest("GET", "https://api.nanit.com/user", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(userResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return &data.User, nil
}

// FetchNotificationSettings - fetches notification settings for a baby
func (c *NanitClient) FetchNotificationSettings(babyUID string) ([]NotificationSetting, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/notification_settings", babyUID), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(notificationSettingsResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Settings, nil
}

// FetchCVR - fetches continuous video recording information for a baby
func (c *NanitClient) FetchCVR(babyUID string) (*CVRInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/cvr", babyUID), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(cvrResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return &data.CVR, nil
}

// FetchFeed - fetches activity feed for a baby
func (c *NanitClient) FetchFeed(babyUID string, limit int) ([]FeedItem, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/feed?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(feedResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Feed, nil
}

// FetchSummary - fetches summary data for a baby on a specific date
func (c *NanitClient) FetchSummary(babyUID string, date string) (*SummaryData, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/summary?date=%s", babyUID, date), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(summaryResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return &data.Summary, nil
}
