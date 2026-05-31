"""Sfera AI SSE Bridge Extension - Intercepts agent responses and sends to SSE server.

When A0 generates a response (from scheduler task or voice dialog),
this extension forwards it to the standalone SSE server on port 32753,
which pushes the event to connected mobile clients.

Architecture:
- Intercepts ALL agent responses via message_loop_end extension point
- Sends agent_response events to SSE server (localhost:32753/push)
- Checks for reminder patterns and sends reminder events
- SSE server delivers events to Android app via SSE streaming

No dependency on A0 ApiHandler for SSE — uses standalone server.
"""

from __future__ import annotations

import re
import time
import json
import logging
from typing import Any

from helpers.extension import Extension
from helpers.print_style import PrintStyle

logger = logging.getLogger(__name__)

# SSE server URL (standalone, not A0)
SSE_SERVER_URL = "http://localhost:32753"

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


def _push_to_sse(event_type: str, data: dict, session_id: str = 'broadcast') -> bool:
    """Send event to standalone SSE server.
    
    Args:
        event_type: reminder, notification, message, agent_response
        data: event payload
        session_id: target session (or 'broadcast' for all)
    
    Returns:
        True if sent successfully, False otherwise
    """
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
            PrintStyle.debug(f"Sfera SSE Bridge: Push {event_type} OK, sent={result.get('sent', 0)}")
            return True
        else:
            PrintStyle.debug(f"Sfera SSE Bridge: Push failed, status={response.status_code}")
            return False
    except Exception as e:
        PrintStyle.debug(f"Sfera SSE Bridge: Push error: {e}")
        return False


class SSEBridge(Extension):
    """Extension that intercepts agent responses and sends to SSE server.

    This is the key component for self-activation:
    - When a scheduler task completes, the agent response is intercepted
    - Response is forwarded to standalone SSE server (port 32753)
    - SSE server pushes event to mobile app
    - App shows notification and/or plays TTS
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
            PrintStyle.standard(
                f"Sfera SSE Bridge: Scheduler task response, forwarding to SSE server"
            )
            _push_to_sse("agent_response", {
                "text": response_text,
                "source": "scheduler",
                "timestamp": int(time.time()),
                "context_id": getattr(self.agent.context, 'id', ''),
                "context_name": getattr(self.agent.context, 'name', ''),
            })

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
                })
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
