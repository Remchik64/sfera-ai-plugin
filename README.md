# Sfera AI Plugin for Agent Zero

Self-activation bridge between Agent Zero and Sfera AI mobile app.
Enables real-time push notifications, reminders, and agent response
delivery via SSE (Server-Sent Events) with Edge TTS audio synthesis.

## Architecture

- **SSE Server** (`sfera_sse_server.py`): Standalone SSE server (configurable port, default 5006)
- **Extension** (`_10_sse_bridge.py`): Intercepts agent responses and forwards to SSE server
- **Edge TTS** (`/tts` endpoint): Generates MP3 audio for SSE event text-to-speech
- **Endpoints**:
  - GET /events?session_id=X — SSE streaming (real-time push to mobile)
  - POST /push — Send event to connected clients (from A0 extension)
  - POST /notify — Receive notification from mobile app (SMS, Telegram, etc.)
  - POST /tts — Generate TTS audio via Edge TTS (text + voice → base64 MP3)
  - GET /status — Connection status
  - GET /health — Health check

## Port Configuration

The SSE server port is **configurable** and not hardcoded:

- **Server**: `--port` argument (default 5006)
- **Extension**: reads `sse_host` and `sse_port` from `default_config.yaml`
- **Android app**: SSE port from SharedPreferences (SettingsActivity)

Default port is 5006, mapped in Docker via `-p 5006:5006`.

## Installation

1. Copy plugin to A0 plugins directory:
   ```bash
   git clone https://github.com/Remchik64/sfera-ai-plugin.git
   cp -r sfera-ai-plugin/* /a0/usr/plugins/sfera_ai/
   ```

2. Enable plugin in A0 WebUI (Settings → Plugins)

3. Start SSE server:
   ```bash
   python sfera_sse_server.py --port 5006
   ```

4. Android app connects to SSE server on configured port (default 5006)

## Edge TTS Voices

- **Svetlana** (female) → `ru-RU-SvetlanaNeural`
- **Dmitry** (male) → `ru-RU-DmitryNeural`

POST /tts example:
```bash
curl -X POST http://localhost:5006/tts \
  -H 'Content-Type: application/json' \
  -d '{"text": "Ренат, пора позвонить маме!", "voice": "Svetlana"}'
```

## Self-Activation Flow

1. User says: "напомни позвонить маме через 10 минут"
2. A0 processes request, extension intercepts response
3. Extension sends event to SSE server via POST /push
4. SSE server pushes event to Android app via SSE streaming
5. App shows notification and/or plays TTS via Edge TTS

## Configuration

`default_config.yaml`:
```yaml
sse_enabled: true
sse_host: localhost
sse_port: 5006
keepalive_interval: 30
pending_events_ttl: 86400
max_pending_events: 100
broadcast_on_scheduler_task: true
```

## License

MIT
