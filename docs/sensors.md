# Sensors

App exposes cam sensors by publishing the updates to MQTT. See `NANIT_MQTT_*` variables in the [.env.sample](../.env.sample) file for configuration.

## Sensor Topics

### Real-time Sensors (from WebSocket)

These sensors are updated in real-time via the WebSocket connection to the camera:

- `nanit/babies/{baby_uid}/temperature` - Temperature in degrees Celsius (float)
- `nanit/babies/{baby_uid}/humidity` - Humidity in percent (float)
- `nanit/babies/{baby_uid}/is_night` - Night mode status (bool)
- `nanit/babies/{baby_uid}/night_light` - Night light on/off status (bool)
- `nanit/babies/{baby_uid}/standby` - Camera standby mode (bool)

### Streaming Sensors

These sensors track the streaming state:

- `nanit/babies/{baby_uid}/stream_url` - Current RTMP stream URL (string)
- `nanit/babies/{baby_uid}/stream_source` - Which path the restreamer is forwarding: "Local", "Remote", or "None" (string)
- `nanit/babies/{baby_uid}/is_stream_alive` - Publisher TCP registration, not recent media (bool). See below.

### Local RTMP health vs MQTT

`is_stream_alive`, `stream_source`, and `camera_online` do not mean go2rtc can read `/local/{baby_uid}`.

- `is_stream_alive` follows RTMP publisher registration. It becomes true when a publisher connects and false when the last publisher for that baby unregisters. A socket that is still registered but has stopped sending media stays true. The MQTT message is retained, so a missed update also leaves the previous value in place.
- `stream_source=Local` means the restreamer prefers the camera's LAN publish. It can stay `Local` while that publish is empty, including when remote standby never came up.
- `camera_online` is a Nanit notification event. It is unrelated to the RTMP publish.

Local RTMP health is a separate signal. It is true only when `/local/{baby_uid}` has a publisher and that publisher has sent media within the last 5 seconds. The process logs `Local RTMP healthy` when that becomes true and `Local RTMP starved` when the publish has been missing for 15 seconds. `GET /health/local-rtmp` on the HTTP server returns 200 only when every baby meets that bar, and 503 otherwise.

That HTTP endpoint is a readiness signal. Do not use it as a liveness probe that restarts the process. When local media is missing, the restreamer re-sends `PUT_STREAMING` on the existing camera websocket (the same request used at startup) so the camera republishes to `/local/{baby_uid}`. A publisher disconnect is re-requested immediately. A registered publisher that sends no media is re-requested after 15 seconds. Further attempts back off from 20 seconds to 90 seconds with jitter. If the camera reports that its mobile connection limit is full, the backoff is about 3 minutes. Remote cloud relay stays a standby and does not clear a local starve.

### Notification Event Sensors

When notification polling is enabled (`NANIT_NOTIFICATIONS_ENABLED=true`), these event sensors are published:

- `nanit/babies/{baby_uid}/motion_timestamp` - Last motion detection time (ISO 8601)
- `nanit/babies/{baby_uid}/sound_timestamp` - Last sound detection time (ISO 8601)
- `nanit/babies/{baby_uid}/camera_online` - Camera online status (bool)
- `nanit/babies/{baby_uid}/is_standing` - Baby standing detected (bool)
- `nanit/babies/{baby_uid}/left_bed` - Toddler left bed detected (bool)
- `nanit/babies/{baby_uid}/alert_zone_triggered` - Alert zone triggered (bool)
- `nanit/babies/{baby_uid}/temperature_alert` - Temperature alert active (bool)
- `nanit/babies/{baby_uid}/humidity_alert` - Humidity alert active (bool)
- `nanit/babies/{baby_uid}/breathing_alert` - Breathing alert active (bool)
- `nanit/babies/{baby_uid}/low_battery` - Low battery alert (bool)

### Sleep Tracking Sensors

When sleep tracking is enabled (`NANIT_SLEEP_TRACKING_ENABLED=true`, default when notifications enabled), these sensors track sleep state:

- `nanit/babies/{baby_uid}/is_asleep` - Baby is currently asleep (bool)
- `nanit/babies/{baby_uid}/in_bed` - Baby is currently in bed (bool)
- `nanit/babies/{baby_uid}/last_sleep_event` - Last sleep event type (string: FELL_ASLEEP, WOKE_UP, PUT_IN_BED, etc.)
- `nanit/babies/{baby_uid}/times_woke_up` - Number of times baby woke up today (int)
- `nanit/babies/{baby_uid}/sleep_interventions` - Number of sleep interventions today (int)
- `nanit/babies/{baby_uid}/awake_time_today` - Total awake time today in minutes (int)
- `nanit/babies/{baby_uid}/sleep_time_today` - Total sleep time today in minutes (int)

### Event Topics

Events are also published to event topics for automation:

- `nanit/babies/{baby_uid}/events/motion` - Motion event payload (JSON)
- `nanit/babies/{baby_uid}/events/sound` - Sound event payload (JSON)
- `nanit/babies/{baby_uid}/events/fell_asleep` - Fell asleep event (JSON)
- `nanit/babies/{baby_uid}/events/woke_up` - Woke up event (JSON)
- ... and other event types

## Home Assistant Integration

When MQTT Discovery is enabled (`NANIT_MQTT_DISCOVERY=true`, default), all sensors are automatically discovered by Home Assistant. See [Home Assistant setup](./home-assistant.md) for more details.

## Troubleshooting

In case you run into trouble and need to see what is going on, you can try using [MQTT Explorer](http://mqtt-explorer.com/).

If sensors show "Unknown" status:

1. **Sleep sensors not updating**: Ensure `NANIT_NOTIFICATIONS_ENABLED=true` and `NANIT_SLEEP_TRACKING_ENABLED=true`
2. **Alert sensors not updating**: These only update when the corresponding event occurs (baby standing, leaving bed, etc.)
3. **Check logs**: Look for polling errors in the application logs
