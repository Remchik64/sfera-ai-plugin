#!/usr/bin/env python3
"""Sfera AI SSE Server - Standalone SSE server for self-activation.

Runs independently from Agent Zero on configurable port (default 5006).
Receives events from A0 extension and pushes to Android clients.

Architecture:
- GET /events?session_id=X — SSE streaming (real-time push to mobile)
- POST /push — send event to connected clients (from A0 extension)
- POST /notify — receive notifications from mobile app (SMS, Telegram, etc.)
- GET /status — show connected clients and pending events

Features:
- Pending events queue for offline clients (TTL 24 hours)
- Auto-cleanup of stale connections (5-minute inactivity threshold)
- Keepalive every 30 seconds
- Broadcast saves to pending for all known sessions (reconnect-safe)
- Thread-safe
"""

from __future__ import annotations

import json
import os
import queue
import time
import threading
import argparse
import logging
from collections import deque
from typing import Optional
from flask import Flask, Request, Response, request, jsonify, stream_with_context

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SferaSSE] %(message)s')


# ──────────────────────────────────────────────────────────
#  SSE Connection Manager
# ──────────────────────────────────────────────────────────

class SferaSSEManager:
    """Manages SSE connections and event queues for self-activation.
    
    Key design decisions for 3+ hour reminder support:
    - _last_active: tracks when each session was last active (keepalive/event)
    - _known_sessions: remembers all sessions that ever connected
    - _pending_timestamps: tracks when pending was last updated for TTL cleanup
    - broadcast() saves to pending for ALL known sessions, not just connected ones
    - cleanup_stale() only removes pending older than TTL (24h), never active connections
    """

    def __init__(self):
        self._connections: dict[str, list[queue]] = {}  # session_id -> [queue, ...]
        self._lock = threading.Lock()
        self._pending: dict[str, deque] = {}  # session_id -> deque of pending events
        self._pending_ttl = 86400  # 24 hours
        self._max_pending = 100
        self._last_active: dict[str, float] = {}  # session_id -> last activity timestamp
        self._known_sessions: set[str] = set()  # all sessions that ever connected
        self._pending_timestamps: dict[str, float] = {}  # session_id -> when pending last updated
        self._stale_threshold = 300  # 5 minutes - for cleaning up metadata

    def touch_activity(self, session_id: str) -> None:
        """Update last activity timestamp for a session (thread-safe)."""
        with self._lock:
            self._last_active[session_id] = time.time()

    def register_session(self, session_id: str) -> None:
        """Register a session as known (for broadcast pending delivery)."""
        with self._lock:
            self._known_sessions.add(session_id)
            self._last_active[session_id] = time.time()

    def connect(self, session_id: str) -> queue:
        """Register a new SSE connection and return its event queue."""
        q = queue.Queue()
        with self._lock:
            self._known_sessions.add(session_id)
            self._last_active[session_id] = time.time()
            if session_id not in self._connections:
                self._connections[session_id] = []
            self._connections[session_id].append(q)
            # Deliver pending events
            if session_id in self._pending:
                while self._pending[session_id]:
                    event = self._pending[session_id].popleft()
                    q.put(event)
                # Don't delete empty pending - keep for TTL tracking
        return q

    def disconnect(self, session_id: str, queue_obj: queue) -> None:
        """Remove an SSE connection. Pending events are preserved for reconnect."""
        with self._lock:
            if session_id in self._connections:
                try:
                    self._connections[session_id].remove(queue_obj)
                except ValueError:
                    pass
                if not self._connections[session_id]:
                    del self._connections[session_id]
            # Keep _known_sessions, _last_active, _pending for reconnect

    def send_event(self, session_id: str, event_type: str, data: dict) -> bool:
        """Send event to a specific session. Returns True if sent, False if queued."""
        with self._lock:
            self._known_sessions.add(session_id)
            self._last_active[session_id] = time.time()
            if session_id in self._connections and self._connections[session_id]:
                for q in self._connections[session_id]:
                    q.put({"event": event_type, "data": data})
                return True
            # Queue for offline delivery
            if session_id not in self._pending:
                self._pending[session_id] = deque(maxlen=self._max_pending)
            self._pending[session_id].append({"event": event_type, "data": data})
            self._pending_timestamps[session_id] = time.time()
            return False

    def broadcast(self, event_type: str, data: dict) -> int:
        """Broadcast event to all connected clients and save to pending for known sessions.
        
        Events are saved to pending for all known sessions not currently connected,
        ensuring delivery on reconnect (critical for 3+ hour reminders).
        """
        sent = 0
        event = {"event": event_type, "data": data}
        now = time.time()
        with self._lock:
            # Send to all currently connected clients
            for session_id, queues in self._connections.items():
                self._last_active[session_id] = now
                for q in queues:
                    q.put(event)
                    sent += 1
            
            # Save to pending for all known sessions that are NOT currently connected
            for session_id in self._known_sessions:
                if session_id not in self._connections or not self._connections[session_id]:
                    if session_id not in self._pending:
                        self._pending[session_id] = deque(maxlen=self._max_pending)
                    self._pending[session_id].append(event)
                    self._pending_timestamps[session_id] = now
        
        return sent

    def get_sessions(self) -> list[str]:
        """Get list of connected session IDs."""
        with self._lock:
            return list(self._connections.keys())

    def get_known_sessions(self) -> list[str]:
        """Get list of all known session IDs (connected + previously seen)."""
        with self._lock:
            return list(self._known_sessions)

    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        with self._lock:
            return sum(len(queues) for queues in self._connections.values())

    def get_pending_count(self, session_id: str) -> int:
        """Get number of pending events for a session."""
        with self._lock:
            return len(self._pending.get(session_id, []))

    def cleanup_stale(self) -> int:
        """Remove stale pending events older than TTL (24 hours).
        
        Only removes pending events that have exceeded the TTL.
        Active connections and their pending events are never removed.
        Also cleans up _last_active and _known_sessions for sessions with
        no connections, no pending, and no recent activity.
        """
        removed = 0
        now = time.time()
        with self._lock:
            # Remove pending older than TTL (24 hours)
            stale_pending = [
                sid for sid, ts in self._pending_timestamps.items()
                if now - ts > self._pending_ttl
            ]
            for sid in stale_pending:
                self._pending.pop(sid, None)
                self._pending_timestamps.pop(sid, None)
                removed += 1
            
            # Clean up _last_active for sessions with no connections, no pending, old activity
            stale_activity = [
                sid for sid, ts in self._last_active.items()
                if sid not in self._connections
                and sid not in self._pending
                and now - ts > self._stale_threshold
            ]
            for sid in stale_activity:
                del self._last_active[sid]
                removed += 1
            
            # Clean up _known_sessions with no connections, no pending, no recent activity
            stale_known = [
                sid for sid in list(self._known_sessions)
                if sid not in self._connections
                and sid not in self._pending
                and sid not in self._last_active
            ]
            for sid in stale_known:
                self._known_sessions.discard(sid)
        
        return removed


# ──────────────────────────────────────────────────────────
#  Flask App
# ──────────────────────────────────────────────────────────

app = Flask(__name__)
sse_manager = SferaSSEManager()


@app.route('/events')
def sse_events():
    """SSE endpoint for real-time push notifications.
    
    GET /events?session_id=X
    Returns SSE stream with events: connected, reminder, notification,
    message, agent_response, keepalive, disconnect.
    """
    session_id = request.args.get('session_id', '').strip()
    if not session_id:
        return jsonify({"error": "Missing session_id"}), 400

    q = sse_manager.connect(session_id)
    app.logger.info(f"SSE: Client connected, session_id={session_id}")

    def generate():
        """Generator that yields SSE events."""
        app.logger.info(f"SSE: Starting stream for session_id={session_id}")

        # Send initial connection event
        yield f"event: connected\ndata: {{\"session_id\": \"{session_id}\"}}\n\n"

        last_keepalive = time.time()
        keepalive_interval = 30

        try:
            while True:
                # Check for keepalive
                now = time.time()
                if now - last_keepalive >= keepalive_interval:
                    yield f"event: keepalive\ndata: {{\"timestamp\": {int(now)}}}\n\n"
                    last_keepalive = now
                    sse_manager.touch_activity(session_id)

                # Try to get event from queue
                try:
                    event = q.get(timeout=1.0)
                    if event is None:
                        yield f"event: disconnect\ndata: {{\"reason\": \"replaced\"}}\n\n"
                        break

                    event_type = event.get("event", "message")
                    data = event.get("data", {})
                    data_json = json.dumps(data, ensure_ascii=False)
                    yield f"event: {event_type}\ndata: {data_json}\n\n"
                    app.logger.info(f"SSE: Sent {event_type} to session_id={session_id}")
                    sse_manager.touch_activity(session_id)

                except queue.Empty:
                    pass  # Timeout, continue for keepalive

        except GeneratorExit:
            app.logger.info(f"SSE: Client disconnected, session_id={session_id}")
        finally:
            sse_manager.disconnect(session_id, q)
            app.logger.info(f"SSE: Stream closed for session_id={session_id}")

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    )




@app.route('/push', methods=['POST'])
def push_event():
    """Push event to connected SSE clients.
    
    POST /push
    Body: {"session_id": "X", "event_type": "reminder", "data": {...}}
    If session_id is empty or 'broadcast', sends to all clients
    and saves to pending for all known sessions.
    """
    body = request.get_json(silent=True) or {}
    session_id = str(body.get('session_id', '')).strip()
    event_type = str(body.get('event_type', 'message')).strip()
    data = body.get('data', {})

    if not isinstance(data, dict):
        data = {"text": str(data)}

    valid_types = {"reminder", "notification", "message", "agent_response"}
    if event_type not in valid_types:
        return jsonify({
            "error": f"Invalid event_type. Must be one of: {', '.join(valid_types)}"
        }), 400

    if session_id == 'broadcast' or not session_id:
        sent = sse_manager.broadcast(event_type, data)
        known = len(sse_manager.get_known_sessions())
        app.logger.info(f"Push: Broadcast {event_type} to {sent} connected, {known} known sessions")
        return jsonify({"ok": True, "sent": sent, "mode": "broadcast", "known_sessions": known})
    else:
        sent = sse_manager.send_event(session_id, event_type, data)
        pending = sse_manager.get_pending_count(session_id)
        app.logger.info(f"Push: Sent {event_type} to session_id={session_id}, sent={sent}, pending={pending}")
        return jsonify({
            "ok": True,
            "sent": 1 if sent else 0,
            "mode": "direct",
            "pending_count": pending
        })


@app.route('/notify', methods=['POST'])
def notify():
    """Receive notification from mobile app and forward to A0.
    
    POST /notify
    Body: {"type": "sms", "from": "Mom", "text": "Call me", "session_id": "X"}
    Forwards to A0 via HTTP request, then pushes result to SSE clients.
    """
    body = request.get_json(silent=True) or {}
    notify_type = str(body.get('type', 'unknown')).strip()
    from_sender = str(body.get('from', '')).strip()
    text = str(body.get('text', '')).strip()
    session_id = str(body.get('session_id', '')).strip()

    app.logger.info(f"Notify: Received {notify_type} from {from_sender}: {text[:50]}")

    # Register session as known if provided (ensures broadcast delivers to it)
    if session_id:
        sse_manager.register_session(session_id)

    # Forward to A0 for processing
    try:
        import requests
        a0_url = os.environ.get('A0_URL', 'http://localhost:80')
        response = requests.post(
            f"{a0_url}/api/chat",
            json={
                "message": f"Пользователь получил уведомление: {notify_type} от {from_sender}: {text}",
                "session_id": session_id,
            },
            timeout=30
        )
        app.logger.info(f"Notify: Forwarded to A0, status={response.status_code}")
    except Exception as e:
        app.logger.warning(f"Notify: Failed to forward to A0: {e}")

    # Also push notification to SSE clients (broadcast saves to pending for known sessions)
    sse_manager.broadcast("notification", {
        "title": f"Новое уведомление: {notify_type}",
        "text": f"От: {from_sender}\n{text}",
        "type": notify_type,
        "timestamp": int(time.time()),
    })

    return jsonify({"ok": True, "type": notify_type, "from": from_sender})


@app.route('/status')
def status():
    """Show SSE connection status."""
    sessions = sse_manager.get_sessions()
    known_sessions = sse_manager.get_known_sessions()
    pending_info = {}
    for sid in known_sessions:
        count = sse_manager.get_pending_count(sid)
        if count > 0:
            pending_info[sid] = count

    return jsonify({
        "status": "running",
        "connected_clients": sse_manager.get_connection_count(),
        "sessions": sessions,
        "known_sessions": known_sessions,
        "pending_events": pending_info,
    })


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "uptime": time.time() - START_TIME})


@app.route('/tts', methods=['POST'])
def tts():
    """Generate TTS audio using Edge TTS.

    POST /tts
    Body: {"text": "Hello", "voice": "Svetlana"}
    Returns: {"audio": "base64...", "engine": "edge", "voice": "ru-RU-SvetlanaNeural", "format": "mp3"}
    """
    body = request.get_json(silent=True) or {}
    text = str(body.get('text', '')).strip()
    voice_name = str(body.get('voice', 'Svetlana')).strip()

    if not text:
        return jsonify({"error": "Text is required"}), 400

    # Voice name mapping
    voice_map = {
        "Svetlana": "ru-RU-SvetlanaNeural",
        "Dmitry": "ru-RU-DmitryNeural",
    }
    voice_id = voice_map.get(voice_name, "ru-RU-SvetlanaNeural")

    try:
        import asyncio
        import edge_tts
        import base64
        import tempfile

        async def generate():
            communicate = edge_tts.Communicate(text, voice_id)
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
                temp_path = f.name
            try:
                await communicate.save(temp_path)
                with open(temp_path, "rb") as audio:
                    data = audio.read()
                return data
            finally:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

        loop = asyncio.new_event_loop()
        try:
            audio_bytes = loop.run_until_complete(generate())
        finally:
            loop.close()

        audio_b64 = base64.b64encode(audio_bytes).decode('utf-8')
        app.logger.info(f"TTS: Generated {len(audio_bytes)} bytes for voice={voice_id}, text='{text[:50]}'")

        return jsonify({
            "audio": audio_b64,
            "engine": "edge",
            "voice": voice_id,
            "format": "mp3",
        })
    except Exception as e:
        app.logger.error(f"TTS: Generation failed: {e}")
        return jsonify({"error": f"TTS generation failed: {str(e)}"}), 500


START_TIME = time.time()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sfera AI SSE Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5006, help='Port to listen on')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    print(f"Sfera AI SSE Server starting on {args.host}:{args.port}")
    print(f"Endpoints:")
    print(f"  GET  /events?session_id=X  — SSE streaming")
    print(f"  POST /push                 — Send event to clients")
    print(f"  POST /notify               — Receive notification from mobile")
    print(f"  GET  /status               — Connection status")
    print(f"  GET  /health               — Health check")

    # Cleanup stale connections periodically
    def cleanup_thread():
        while True:
            time.sleep(60)
            removed = sse_manager.cleanup_stale()
            if removed:
                app.logger.info(f"Cleaned up {removed} stale entries (pending older than 24h / metadata)")

    t = threading.Thread(target=cleanup_thread, daemon=True)
    t.start()

    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)
