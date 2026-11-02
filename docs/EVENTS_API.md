# Nanit API Endpoints

The Nanit REST API provides several endpoints for monitoring baby activity. All endpoints documented here have been implemented in `pkg/client/rest.go`.

**Endpoint summary:**
- `/messages` - Returns MOTION and SOUND events only
- `/events` - Returns sleep events, visit events, and more
- `/stats/latest` - Sleep state machine (ASLEEP/AWAKE/PARENT_INTERVENTION)
- `/events/last` - Quick check for latest event
- `/notification_settings` - Shows what notifications are enabled

## Not Available via REST API

**Crying detection (`CAMERA_CRY_DETECTION`) is NOT available via any REST API endpoint.** It is only delivered through Firebase Cloud Messaging (FCM) push notifications, which requires Firebase credentials that cannot be obtained without compromising Nanit's app security. This limitation is permanent for REST API polling.

---

## All Known Endpoints (from APK decompilation)

### Baby/Camera Endpoints
```
GET  /babies/{baby_uid}?include=camera     # Baby + camera details
GET  /babies/{baby_uid}/events             # Event history
GET  /babies/{baby_uid}/events/last        # Last event (quick)
GET  /babies/{baby_uid}/messages           # Notifications (MOTION/SOUND only)
GET  /babies/{baby_uid}/stats/latest       # Sleep states
GET  /babies/{baby_uid}/notification_settings  # What's enabled
GET  /babies/{baby_uid}/feed               # Activity feed (needs params)
GET  /babies/{baby_uid}/summary            # Summary (needs params)
```

### Authentication
```
POST /login                                # Login with email/password
POST /tokens/refresh                       # Refresh access token
POST /mfa/enroll/session                   # MFA enrollment
POST /mfa/resend                           # Resend MFA code
```

### Other Useful Endpoints
```
GET  /user                                 # Current user info
POST /devices/{device}                     # Register push device
GET  /babies/{baby_uid}/cvr                # Continuous video recording
GET  /moments/babies/{baby_uid}/events/{event_uid}  # Event details
```

---

## Events Endpoint

```
GET https://api.nanit.com/babies/{baby_uid}/events?limit={limit}
```

**Headers:**
- `Authorization: Bearer {auth_token}`
- `Content-Type: application/json`

## Event Types

| Key | Internal Key | Title | Use Case |
|-----|--------------|-------|----------|
| FELL_ASLEEP | FELL_ASLEEP | Baby fell asleep | Sleep tracking |
| WOKE_UP | WOKE_UP | Baby woke up | Sleep tracking |
| PUT_IN_BED | PUT_IN_BED | Parent put baby in bed | Bedtime tracking |
| PUT_TO_SLEEP | PUT_TO_SLEEP | Baby is put to sleep | Bedtime tracking |
| REMOVED | REMOVED | Parent took baby out | Presence tracking |
| REMOVED_ASLEEP | REMOVED_ASLEEP | Parent took baby out (asleep) | Presence tracking |
| VISIT | VISIT | Parent approaching | Activity tracking |
| VISIT | VISIT_FELL_ASLEEP | Parent visited, baby fell asleep | Activity tracking |
| VISIT | VISIT_WOKE_UP | Parent visited, baby woke up | Activity tracking |
| MOTION | MOTION | Motion detected | Already supported |
| SOUND | SOUND | Sound detected | Already supported |

## Sample Response

```json
{
  "key": "FELL_ASLEEP",
  "internal_key": "FELL_ASLEEP",
  "title": "Baby fell asleep",
  "time": 1767969084.0,
  "begin_ts": 1767969074.0,
  "end_ts": 1767969094.0,
  "baby_uid": "xxxxxxxx",
  "camera_uid": "XXXXXXXXXXXXX",
  "uid": "abc123",
  "confidence": 1.0,
  "source": "sm",
  "clip": true,
  "playlist": { ... },
  "raw_videos": { ... }
}
```

## Proposed Home Assistant Sensors

### Binary Sensors

1. **is_asleep** - Baby sleep state
   - ON: After FELL_ASLEEP event
   - OFF: After WOKE_UP event

2. **in_bed** - Baby in crib
   - ON: After PUT_IN_BED event
   - OFF: After REMOVED or REMOVED_ASLEEP event

### Event Sensors

Could also expose as Home Assistant events for automations:
- `nanit_baby_fell_asleep`
- `nanit_baby_woke_up`
- `nanit_baby_put_in_bed`
- `nanit_baby_removed`
- `nanit_parent_visit`

## Implementation Notes

1. Poll `/events` endpoint alongside `/messages`
2. Track state based on event sequence (FELL_ASLEEP -> WOKE_UP)
3. Use event `time` field for ordering
4. Deduplicate by event `uid`

## Implemented Endpoints

All endpoints listed above have been implemented in `pkg/client/rest.go`:

| Method | Endpoint | Go Function |
|--------|----------|-------------|
| `POST` | `/login` | `Login()` |
| `POST` | `/tokens/refresh` | `RenewSession()` |
| `GET` | `/babies` | `FetchBabies()` |
| `GET` | `/babies/{uid}?include=camera` | `FetchBabyWithCamera()` |
| `GET` | `/babies/{uid}/events` | `FetchSleepEvents()` |
| `GET` | `/babies/{uid}/events/last` | `FetchLastEvent()` |
| `GET` | `/babies/{uid}/messages` | `FetchMessages()` |
| `GET` | `/babies/{uid}/stats/latest` | `FetchSleepStats()` |
| `GET` | `/babies/{uid}/notification_settings` | `FetchNotificationSettings()` |
| `GET` | `/babies/{uid}/cvr` | `FetchCVR()` |
| `GET` | `/babies/{uid}/feed` | `FetchFeed()` |
| `GET` | `/babies/{uid}/summary` | `FetchSummary()` |
| `GET` | `/user` | `FetchUser()` |
