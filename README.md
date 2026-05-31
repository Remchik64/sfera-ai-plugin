# Sfera AI Plugin for Agent Zero

Self-activation bridge between Agent Zero and Sfera AI mobile app.

## Architecture

- **SSE Server** (`sfera_sse_server.py`): Standalone SSE server on port 32753
- **Extension** (`_10_sse_bridge.py`): Intercepts agent responses and forwards to SSE server
- **Endpoints**:
  - GET /events?session_id=X — SSE streaming
  - POST /push — Send event to clients
  - POST /notify — Receive notification from mobile
  - GET /status — Connection status
  - GET /health — Health check

## Installation

1. Copy plugin to A0 plugins directory:
   ```bash
   cp -r sfera-ai-plugin /a0/usr/plugins/sfera_ai
   ```

2. Enable plugin in A0 WebUI (Settings → Plugins)

3. Start SSE server:
   ```bash
   python sfera_sse_server.py --port 32753
   ```

4. Android app connects to SSE server on port 32753

## Self-Activation Flow

1. User says: "remind me to call mom in 10 minutes"
2. A0 processes request, extension intercepts response
3. Extension sends event to SSE server via POST /push
4. SSE server pushes event to Android app
5. App shows notification and/or plays TTS

## License

MIT
