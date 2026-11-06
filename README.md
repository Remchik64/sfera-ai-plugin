# Sfera AI Plugin — мост пушей между Agent Zero и телефоном

Отдельный SSE-сервер, который доставляет события на Android-приложение Sfera
и озвучивает их через Edge TTS.

> Проверено чтением кода и живыми запросами 2026-11-06: `/events`, `/push`,
> `/status`, `/health` работают; доставка недоставленных событий при реконнекте
> подтверждена. `/notify` содержит дефект — см. «Известные проблемы».

## Что это

Два компонента в одном репозитории:

1. **`sfera_sse_server.py`** — самостоятельный Flask-сервер (порт **5006**)
2. **`extensions/python/message_loop_end/_10_sse_bridge.py`** — расширение A0,
   перехватывающее ответы агента и отправляющее их на SSE-сервер

## Эндпоинты

| Метод | Путь | Что делает |
|---|---|---|
| GET | `/events?session_id=X` | SSE-поток: `connected`, `reminder`, `notification`, `message`, `agent_response`, `keepalive`, `disconnect` |
| POST | `/push` | отправить событие: `{session_id, event_type, data}` |
| POST | `/notify` | приём уведомления с телефона (**см. проблему 1**) |
| POST | `/tts` | Edge TTS: `{text, voice}` → `{audio: base64, engine, voice, format}` |
| GET | `/status` | подключённые и известные сессии, счётчики pending |
| GET | `/health` | `{status, uptime}` |

`event_type` для `/push`: `reminder`, `notification`, `message`, `agent_response`.
`session_id: "broadcast"` — всем; иначе адресно одной сессии.

## Ключевая механика: доставка после офлайна

Сервер помнит **все** сессии, которые когда-либо подключались
(`known_sessions`), и складывает для них события в очередь `pending`.

- TTL очереди — 24 часа (`pending_events_ttl`)
- Максимум событий на сессию — 100 (`max_pending_events`)
- `broadcast` пишет в pending для всех известных сессий, а не только для
  подключённых — поэтому напоминание через 3 часа дойдёт после реконнекта

Проверено вживую: событие, отправленное офлайн-клиенту, пришло сразу после
подключения к `/events`.

## Установка

```bash
git clone https://github.com/Remchik64/sfera-ai-plugin.git
cp -r sfera-ai-plugin/* /a0/usr/plugins/sfera_ai/
```

Включить плагин в A0 WebUI (Settings → Plugins) и запустить SSE-сервер:

```bash
python sfera_sse_server.py --port 5006
```

Порт должен быть проброшен из Docker (`-p 5006:5006`), иначе телефон не
подключится.

## Зависимости

| Пакет | Зачем | Обязателен |
|---|---|---|
| `flask` | SSE-сервер | да |
| `requests` | форвард уведомлений | да |
| `edge-tts` | `/tts` и озвучка событий | для TTS |
| `pyyaml` | чтение конфига расширением | да |

```bash
pip install flask requests pyyaml edge-tts
```

`requirements.txt` в репозитории отсутствовал — это исправлено.

## Конфигурация

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

Расширение читает `sse_host` и `sse_port` из этого файла, поэтому порт не
зашит в код.

## Поток самопроактивации

```
1. «напомни позвонить маме через 3 часа»
2. A0 ставит будильник, отвечает пользователю
3. Через 3 часа звонит AlarmManager на телефоне
4. Приложение шлёт POST /notify на SSE-сервер
5. (задумано) сервер форвардит в A0 → A0 формирует текст напоминания
6. Расширение _10_sse_bridge.py перехватывает ответ агента
7. POST /push → SSE → телефон: уведомление + озвучка
```

## Голоса Edge TTS

```
Svetlana → ru-RU-SvetlanaNeural
Dmitry   → ru-RU-DmitryNeural
```

Фактически в бесплатном каталоге Edge только эти два русских голоса.

```bash
curl -X POST http://localhost:5006/tts \
  -H 'Content-Type: application/json' \
  -d '{"text": "Ренат, пора позвонить маме!", "voice": "Svetlana"}'
```

## Известные проблемы

1. **`/notify` шлёт на несуществующий эндпоинт.** В `sfera_sse_server.py`
   (`notify()`) запрос идёт на `{A0_URL}/api/chat`, но в Agent Zero такого
   маршрута нет — есть `/api/api_message`. Форвард уведомлений с телефона в
   мозг **молча падает в 404**. Пуш на телефон при этом работает.
2. **`/push` и `/notify` без авторизации.** Любой в сети может отправить
   поддельное «уведомление от ассистента» на телефон и услышать его голосом.
3. **Ложные пуши.** `_is_scheduler_context()` считает планировщиком **любой**
   контекст с непустым `name`, а `helpers/subagents.py` имя задаёт. Ответ
   субагента уйдёт на телефон как «задача планировщика».
4. **SSE всегда по HTTP.** Android-клиент (`SSEClient.kt`) принудительно
   ставит `http://`, срезая схему — HTTPS-туннель сломает пуши.
5. **Порт 32753 в старых записях** — историческое значение. Актуальный порт
   **5006** (сервер, Android `DEFAULT_SSE_PORT`, Docker-маппинг).

## Проверка

```bash
curl http://localhost:5006/health
curl http://localhost:5006/status
curl -X POST http://localhost:5006/push -H 'Content-Type: application/json' \
  -d '{"session_id":"test","event_type":"message","data":{"text":"проверка"}}'
```

## Связанные репозитории

- **`agent_rema`** — Android-приложение и голосовой плагин A0
- **`sfera-voice`** — универсальный слой озвучки (RouterAI, Edge, локальные CLI)
  с маппингом голосов и современными русскими голосами

## Лицензия

MIT
