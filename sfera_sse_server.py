#!/usr/bin/env python3
"""Sfera AI SSE Server - Standalone SSE server for self-activation.

Runs independently from Agent Zero on port 32753.
Receives events from A0 extension and pushes to Android clients.

Architecture:
- GET /events?session_id=X — SSE streaming (real-time push to mobile)
- POST /push — send event to connected clients (from A0 extension)
- POST /notify — receive notifications from mobile app (SMS, Telegram, etc.)
- GET /status — show connected clients and pending events

Features:
- Pending events queue for offline clients
- Auto-cleanup of stale connections
- Keepalive every 30 seconds
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
from flask import Flask, Request, Response, request, jsonify

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SferaSSE] %(message)s')


# ──────────────────────────────────────────────────────────
#  SSE Connection Manager
# ──────────────────────────────────────────────────────────

class SferaSSEManager:
    """Manages SSE connections and event queues for self-activation."""

    def __init__(self):
        self._connections: dict[str, list[queue]] = {}  # session_id -> [queue, ...]
        self._lock = threading.Lock()
        self._pending: dict[str, deque] = {}  # session_id -> deque of pending events
        self._pending_ttl = 86400  # 24 hours
        self._max_pending = 100

    def connect(self, session_id: str) -> queue:
        """Register a new SSE connection and return its event queue."""
        q = queue.Queue()
        with self._lock:
            if session_id not in self._connections:
                self._connections[session_id] = []
            self._connections[session_id].append(q)
            # Deliver pending events
            if session_id in self._pending:
                while self._pending[session_id]:
                    event = self._pending[session_id].popleft()
                    q.put(event)
        return q

    def disconnect(self, session_id: str, queue_obj: queue) -> None:
        """Remove an SSE connection."""
        with self._lock:
            if session_id in self._connections:
                try:
                    self._connections[session_id].remove(queue_obj)
                except ValueError:
                    pass
                if not self._connections[session_id]:
                    del self._connections[session_id]

    def send_event(self, session_id: str, event_type: str, data: dict) -> bool:
        """Send event to a specific session. Returns True if sent, False if queued."""
        with self._lock:
            if session_id in self._connections and self._connections[session_id]:
                for q in self._connections[session_id]:
                    q.put({"event": event_type, "data": data})
                return True
            # Queue for offline delivery
            if session_id not in self._pending:
                self._pending[session_id] = deque(maxlen=self._max_pending)
            self._pending[session_id].append({"event": event_type, "data": data})
            return False

    def broadcast(self, event_type: str, data: dict) -> int:
        """Broadcast event to all connected clients. Returns number of clients reached."""
        sent = 0
        with self._lock:
            for session_id, queues in self._connections.items():
                for q in queues:
                    q.put({"event": event_type, "data": data})
                    sent += 1
        return sent

    def get_sessions(self) -> list[str]:
        """Get list of connected session IDs."""
        with self._lock:
            return list(self._connections.keys())

    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        with self._lock:
            return sum(len(queues) for queues in self._connections.values())

    def get_pending_count(self, session_id: str) -> int:
        """Get number of pending events for a session."""
        with self._lock:
            return len(self._pending.get(session_id, []))

    def cleanup_stale(self) -> int:
        """Remove stale pending events. Returns count of removed sessions."""
        removed = 0
        now = time.time()
        with self._lock:
            stale = [
                sid for sid, events in self._pending.items()
                if not events
            ]
            for sid in stale:
                del self._pending[sid]
                removed += 1
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

                except queue.Empty:
                    pass  # Timeout, continue for keepalive

        except GeneratorExit:
            app.logger.info(f"SSE: Client disconnected, session_id={session_id}")
        finally:
            sse_manager.disconnect(session_id, q)
            app.logger.info(f"SSE: Stream closed for session_id={session_id}")

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        }
    )


@app.route('/push', methods=['POST'])
def push_event():
    """Push event to connected SSE clients.
    
    POST /push
    Body: {"session_id": "X", "event_type": "reminder", "data": {...}}
    If session_id is empty or 'broadcast', sends to all clients.
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
        app.logger.info(f"Push: Broadcast {event_type} to {sent} clients")
        return jsonify({"ok": True, "sent": sent, "mode": "broadcast"})
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

    # Forward to A0 for processing
    # A0 will respond via extension which sends to /push
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

    # Also push notification to SSE clients
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
    pending_info = {}
    for sid in sessions:
        count = sse_manager.get_pending_count(sid)
        if count > 0:
            pending_info[sid] = count

    return jsonify({
        "status": "running",
        "connected_clients": sse_manager.get_connection_count(),
        "sessions": sessions,
        "pending_events": pending_info,
    })


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "uptime": time.time() - START_TIME})


START_TIME = time.time()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sfera AI SSE Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=32753, help='Port to listen on')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    import os
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
                app.logger.info(f"Cleaned up {removed} stale sessions")

    t = threading.Thread(target=cleanup_thread, daemon=True)
    t.start()

    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)
