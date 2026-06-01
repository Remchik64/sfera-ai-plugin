"""Sfera AI SSE Bridge Extension - Intercepts agent responses and sends to SSE server.

When A0 generates a response (from scheduler task or voice dialog),
this extension forwards it to the standalone SSE server,
which pushes the event to connected mobile clients.

Architecture:
- Intercepts ALL agent responses via message_loop_end extension point
- Sends agent_response events to SSE server ({sse_host}:{sse_port}/push)
- Checks for reminder patterns and sends reminder events
- SSE server delivers events to Android app via SSE streaming
- Port and host are configurable via default_config.yaml (sse_host, sse_port)

Reminder flow (3+ hours):
1. User says 'напомни позвонить маме через 3 часа'
2. A0 sets alarm for +3 hours
3. A0 responds 'Хорошо, напомню через 3 часа'
4. After 3 hours, alarm fires
5. Mobile app sends POST /notify to SSE server
6. SSE server forwards to A0 (POST /api/chat)
7. A0 generates 'Ренат, пора позвонить маме!'
8. This extension intercepts the response
9. Extension sends to SSE server (targeted or broadcast)
10. SSE server delivers to phone (with pending if disconnected)

No dependency on A0 ApiHandler for SSE — uses standalone server.
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
import yaml
from typing import Any

from helpers.extension import Extension
from helpers.print_style import PrintStyle

logger = logging.getLogger(__name__)

# Default SSE server settings (overridden by default_config.yaml)
_DEFAULT_SSE_HOST = "localhost"
_DEFAULT_SSE_PORT = 5006

# Russian reminder patterns for detection
_REMINDER_KEYWORDS = [
    'напомн', 'напоминани', 'напоминай', 'напомин',
    'поставлю напомин', 'создам напомин',
]

_TIME_PATTERNS = [
    r'через\s+\d+\s*(?:минут|мин|час|часов|секунд|сек)[а-яё]*',
    r'через\s+(?:один|одну|два|две|три|четыре|пять|шесть|семь|восемь|девять|десять)\s*(?:час|часа|часов|минут|минуты|мин)[а-яё]*',
    r'через\s+час[а-яё]*',
    r'через\s+пол\s*часа',
    r'(?:в|на)\s+\d{1,2}:\d{2}',
]


def _load_sse_config() -> tuple[str, int]:
    """Load SSE host and port from plugin config file.
    
    Tries multiple paths to find default_config.yaml:
    1. Plugin directory (relative to extension file)
    2. /a0/usr/plugins/sfera_ai/default_config.yaml
    3. Fallback to defaults
    """
    config_paths = [
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'default_config.yaml'),
        '/a0/usr/plugins/sfera_ai/default_config.yaml',
    ]
    
    for path in config_paths:
        path = os.path.normpath(path)
        if os.path.isfile(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                host = str(config.get('sse_host', _DEFAULT_SSE_HOST))
                port = int(config.get('sse_port', _DEFAULT_SSE_PORT))
                return host, port
            except Exception as e:
                logger.debug(f"Sfera SSE Bridge: Config load error from {path}: {e}")
    
    return _DEFAULT_SSE_HOST, _DEFAULT_SSE_PORT


# Load config at module level
_SSE_HOST, _SSE_PORT = _load_sse_config()
SSE_SERVER_URL = f"http://{_SSE_HOST}:{_SSE_PORT}"


def _detect_reminder(text: str) -> bool:
    """Check if text contains reminder patterns."""
    if not text:
        return False
    text_lower = text.lower()
    has_keyword = any(kw in text_lower for kw in _REMINDER_KEYWORDS)
    if not has_keyword:
        return False
    return True  # Keyword is sufficient


def _extract_reminder_text(text: str) -> str:
    """Extract the reminder text from the response."""
    if not text:
        return text
    m = re.search(
        r'(?:напомн|напоминани|напоминай|напомин)[а-яё]*\s+(.+)$',
        text, re.IGNORECASE
    )
    if m:
        result = m.group(1)
        result = re.sub(r'через\s+(?:\d+\s*)?(?:минут|мин|час|часов|секунд|сек)[а-яё]*\s*', '', result, flags=re.IGNORECASE)
        result = re.sub(r'(?:в|на)\s+\d{1,2}:\d{2}\s*', '', result)
        result = result.strip(' ,.!:;-')
        return result if len(result) >= 2 else text.strip()
    return text.strip()


def _is_scheduler_context(agent) -> bool:
    """Check if this agent is running in a scheduler task context."""
    if not agent or not hasattr(agent, 'context'):
        return False
    context = agent.context
    context_name = getattr(context, 'name', '') or ''
    context_id = getattr(context, 'id', '') or ''
    # Scheduler tasks have context_id that doesn't start with 'voice_'
    if context_name and not context_id.startswith('voice_'):
        return True
    return False


def _get_known_sessions() -> list[str]:
    """Query SSE server for known (connected + previously seen) session IDs.
    
    Returns list of session IDs, or empty list if server unreachable.
    Used to convert broadcast into targeted delivery.
    """
    try:
        import requests
        response = requests.get(
            f"{SSE_SERVER_URL}/status",
            timeout=3
        )
        if response.status_code == 200:
            data = response.json()
            # known_sessions includes both connected and previously seen sessions
            return data.get('known_sessions', [])
    except Exception as e:
        logger.debug(f"Sfera SSE Bridge: Failed to get known sessions: {e}")
    return []


def _push_to_sse(event_type: str, data: dict, session_id: str = 'broadcast') -> bool:
    """Send event to standalone SSE server.
    
    Args:
        event_type: reminder, notification, message, agent_response
        data: event payload
        session_id: target session (or 'broadcast' for all)
    
    If session_id is 'broadcast', tries to resolve known sessions from
    SSE server and sends targeted events to each. Falls back to broadcast
    if server query fails. The server-side broadcast also saves to pending
    for all known sessions (reconnect-safe).
    
    Returns:
        True if sent successfully, False otherwise
    """
    if session_id == 'broadcast':
        # Try targeted delivery to known sessions first
        known_sessions = _get_known_sessions()
        if known_sessions:
            success_count = 0
            for sid in known_sessions:
                try:
                    import requests
                    response = requests.post(
                        f"{SSE_SERVER_URL}/push",
                        json={
                            "session_id": sid,
                            "event_type": event_type,
                            "data": data,
                        },
                        timeout=5
                    )
                    if response.status_code == 200:
                        success_count += 1
                except Exception:
                    pass
            PrintStyle.debug(
                f"Sfera SSE Bridge: Targeted {event_type} to {success_count}/{len(known_sessions)} sessions"
            )
            return success_count > 0
        
        # Fall back to broadcast (server saves to pending for all known sessions)
        try:
            import requests
            response = requests.post(
                f"{SSE_SERVER_URL}/push",
                json={
                    "session_id": "broadcast",
                    "event_type": event_type,
                    "data": data,
                },
                timeout=5
            )
            if response.status_code == 200:
                result = response.json()
                PrintStyle.debug(
                    f"Sfera SSE Bridge: Broadcast {event_type} OK, sent={result.get('sent', 0)}, known={result.get('known_sessions', 0)}"
                )
                return True
            else:
                PrintStyle.debug(f"Sfera SSE Bridge: Broadcast failed, status={response.status_code}")
                return False
        except Exception as e:
            PrintStyle.debug(f"Sfera SSE Bridge: Broadcast error: {e}")
            return False
    
    # Direct targeted delivery
    try:
        import requests
        response = requests.post(
            f"{SSE_SERVER_URL}/push",
            json={
                "session_id": session_id,
                "event_type": event_type,
                "data": data,
            },
            timeout=5
        )
        if response.status_code == 200:
            result = response.json()
            PrintStyle.debug(f"Sfera SSE Bridge: Push {event_type} to {session_id} OK, sent={result.get('sent', 0)}")
            return True
        else:
            PrintStyle.debug(f"Sfera SSE Bridge: Push to {session_id} failed, status={response.status_code}")
            return False
    except Exception as e:
        PrintStyle.debug(f"Sfera SSE Bridge: Push to {session_id} error: {e}")
        return False


class SSEBridge(Extension):
    """Extension that intercepts agent responses and sends to SSE server.

    This is the key component for self-activation:
    - When a scheduler task completes, the agent response is intercepted
    - Response is forwarded to standalone SSE server (configurable port)
    - SSE server pushes event to mobile app (with pending for offline clients)
    - App shows notification and/or plays TTS
    
    SSE host and port are read from default_config.yaml (sse_host, sse_port).
    """

    async def execute(self, loop_data=None, **kwargs):
        if not self.agent:
            return

        # Get the agent's response text
        response_text = ""
        if loop_data and hasattr(loop_data, 'last_response'):
            response_text = loop_data.last_response or ""

        if not response_text or not response_text.strip():
            return

        # Check if this is a scheduler task context
        is_scheduler = _is_scheduler_context(self.agent)

        # For scheduler tasks: always send agent_response event
        if is_scheduler:
            context_id = getattr(self.agent.context, 'id', '')
            context_name = getattr(self.agent.context, 'name', '')
            PrintStyle.standard(
                f"Sfera SSE Bridge: Scheduler task response, forwarding to SSE server ({SSE_SERVER_URL}), context={context_name}"
            )
            
            # Try to find session_id from context_id
            # Scheduler tasks may have context_id that maps to a known session
            session_id = 'broadcast'
            if context_id and not context_id.startswith('voice_'):
                # Check if this context_id is a known SSE session
                known = _get_known_sessions()
                if context_id in known:
                    session_id = context_id
                    PrintStyle.standard(
                        f"Sfera SSE Bridge: Resolved scheduler context to session_id={session_id}"
                    )
            
            _push_to_sse("agent_response", {
                "text": response_text,
                "source": "scheduler",
                "timestamp": int(time.time()),
                "context_id": context_id,
                "context_name": context_name,
            }, session_id=session_id)

            # Also check for reminder patterns
            if _detect_reminder(response_text):
                reminder_text = _extract_reminder_text(response_text)
                PrintStyle.standard(
                    f"Sfera SSE Bridge: Scheduler reminder: '{reminder_text}'"
                )
                _push_to_sse("reminder", {
                    "reminder_text": reminder_text,
                    "source": "scheduler",
                    "timestamp": int(time.time()),
                    "full_response": response_text,
                }, session_id=session_id)
            return

        # For regular responses (voice sessions): check for reminder patterns
        if _detect_reminder(response_text):
            reminder_text = _extract_reminder_text(response_text)
            PrintStyle.standard(
                f"Sfera SSE Bridge: Reminder detected: '{reminder_text}'"
            )

            # Try to find session_id from context
            context_id = getattr(self.agent.context, 'id', '')
            session_id = 'broadcast'
            if context_id.startswith('voice_'):
                session_id = context_id.replace('voice_', '', 1)

            _push_to_sse("reminder", {
                "reminder_text": reminder_text,
                "source": "voice",
                "timestamp": int(time.time()),
                "full_response": response_text,
            }, session_id=session_id)
