# TOOLS.md — инструментарий agent_bridge

Полный справочник по всем инструментам локального MCP-моста между тремя агентами (Claude Code / cclocal, Kimi CLI и oclocal / OpenClaude на локальной модели). Обзорная документация — в [README.md](README.md), список известных ограничений — в [TODO.md](TODO.md). Файл описывает поведение текущего HEAD; изменения в работе помечены.

## Состав

```text
Claude Code сессия ── MCP stdio ──► server.py (AGENT_ID=claude) ─┐
                                                                  ├──► bridge.db (SQLite, WAL)
Kimi CLI сессия ───── MCP stdio ──► server.py (AGENT_ID=kimi) ───┤
                                                                  │
OpenClaude сессия ── MCP stdio ──► server.py (AGENT_ID=local) ───┘
                                       ▲  autostart при загрузке MCP
                             systemd/listener.py   (по одному на агента, flock-синглтон)
                                       │  scan → dispatch → wait
                             systemd/on_message.py (one-shot supervisor на сообщение)
                                       │
              cclocal_headless.sh / kimi_headless.sh / oclocal_headless.sh → headless worker
```

Два слоя: **MCP-слой** (`server.py`, 8 инструментов, все правила протокола на стороне SQLite) и **слой доставки** (`systemd/`: listener → supervisor → launcher → worker). Sender identity берётся из env `AGENT_ID` ∈ `{claude, kimi, local}`; аргументы инструментов не могут её подменить. Имена `cclocal`/`oclocal` — только launcher/префиксы отчётов соответствующих сторон, `AGENT_ID=cclocal` отклоняется.

---

## 1. MCP-инструменты (8)

Общие константы-ограничения (`server.py`): long-poll ≤ 30 с, `response_timeout_seconds` 1..3600 (дефолт 300), lease 30..3600, conversation: `max_turns` 2..20 (дефолт 6), TTL 60..86400 с (дефолт 14400), виды сообщений диалога `task | result | notice`. Ошибки вызывают `ValueError` (MCP вернёт `is_error=true`).

### send_message

```text
send_message(recipient, text, reply_to=None, response_timeout_seconds=300,
             dedup_key=None, conversation_id=None, kind=None)
```

- `recipient` ∈ `{kimi, claude}`, обязан отличаться от отправителя.
- `reply_to` — id сообщения, на которое отвечаем (связывание цепочки).
- `response_timeout_seconds` клампится в 1..3600 и сохраняется как `response_deadline = now + timeout` — **чистые метаданные**: ничего не таймаутится, не удаляется и не пересылается; просроченный ответ остаётся валидным.
- `dedup_key`: повторный вызов с тем же `(sender, dedup_key)` возвращает существующее сообщение с `duplicate=true`, новая строка не вставляется.
- `conversation_id` + `kind`: сообщение внутри автодиалога. `kind` обязателен при `conversation_id`; иначе наследуется из диалога `reply_to`. Проверки атомарны: диалог активен и не истёк; `result` отвечает `task`, follow-up `task` отвечает `result`, адресат — отправитель отвечаемого; ровно одно in-flight; превышение лимита ходов переводит диалог в `needs_human` и отклоняет отправку.
- Обычный `send_message` без `kind` — простое письмо, автономным не становится.

Возврат (простое письмо): `{message_id, sender, response_deadline, duplicate}`. В диалоге: `{message_id, conversation_id, kind, turn, sender, response_deadline, duplicate}`.

### receive_messages

```text
receive_messages(limit=20, wait_seconds=0)
```

- Возвращает необработанные письма адресата `AGENT_ID`, старые первыми.
- Письма в активном чужом claim скрыты. Long-poll до `min(wait_seconds, 30)` с (шаг опроса 0.25 с).
- Письма остаются непрочитанными до `mark_read`.
- Поля каждого письма: `id, sender, text, reply_to, created_at, response_deadline, claim_expires_at, owned_by_current_worker, conversation_id, kind, turn`; плюс `cursor` — id последнего возвращённого.

### claim_message

```text
claim_message(message_id, lease_seconds=300)   # lease клампится 30..3600
```

Атомарный claim перед headless-исполнением. Повтор вызова владельцем продлевает аренду; истёкший claim подхватывает любой процесс получателя. Чужой активный claim нельзя перехватить, подтвердить или освободить.

Возврат при успехе: `{message_id, status:"claimed", claimed:true, claim_expires_at, message:{...полный payload...}}`. Иначе `claimed:false` и `status` ∈ `not_found | acknowledged | claimed_by_other`.

### release_message

```text
release_message(message_id)
```

Вернуть свой активный claim без подтверждения (письмо остаётся в общем inbox). Возврат: `released:true, status:"released"` либо `released:false` со `status` ∈ `not_found | acknowledged | not_claimed | claimed_by_other`.

### mark_read

```text
mark_read(message_ids)
```

Подтверждение по id; подтвердить может только получатель (`AGENT_ID`). Уже прочитанные, чужие и в активном чужом claim id молча пропускаются. Возврат: `{acknowledged: N}`.

### start_conversation

```text
start_conversation(recipient, goal, max_turns=6, expires_in_seconds=14400,
                   response_timeout_seconds=300, dedup_key=None)
```

Создаёт автодиалог (`goal`, инициатор = вызывающий агент) и постит корневое `task` ходом 1 — слушатель получателя подхватит его без ручной фразы. Лимит ходов считает каждый worker-turn (включая ревью result'ов); исчерпание/истечение → `needs_human`. `dedup_key` делает повторный старт идемпотентным.

Возврат: `{conversation_id, message_id, kind:"task", turn:1, max_turns, state:"active", expires_at, response_deadline, duplicate}`.

### conversation

```text
conversation(action, conversation_id)   # action: status | cancel | complete | needs_human
```

Управление диалогом — только его двумя участникам.

- `status` — `state, turn_count/max_turns, expires_at, последние переходы, in-flight message id`; **never** goal text и message text (без payload).
- `cancel` — остановить дальнейшую автодиспетчеризацию (из `active/blocked/needs_human`; побочные эффекты не откатываются).
- `complete` — цель достигнута (только из `active`).
- `needs_human` — нужно решение человека (только из `active`).
- Повтор того же терминального состояния — идемпотентный no-op; иной терминальный переход отклоняется: агенты не могут бороться за исход.

### listener

```text
listener(action)   # action: status | progress | enable | disable
```

Управление **собственным** фоновым слушателем текущего агента:

- `status` — машинный `status()` слушателя (поля ниже).
- `progress` — человекочитаемая однострочка, ровно та, что рисует statusline cclocal: `{"line": ...}`. Фолбэк `[<agent>] unknown`, вызов не падает никогда.
- `enable` — снять disabled-маркер и идемпотентно запустить (дубликатов нет — flock; работает даже при `AGENT_BRIDGE_AUTO_LISTEN=0`).
- `disable` — маркер-файл; слушатель берёт новых задач, дорабатывает текущего воркера и завершается. **Никакой PID никогда не убивается.**

Фактическая диспетчеризация по-прежнему регулируется гейтом `auto.<agent>` в `systemd/config.json`.

---

## 2. CLI `systemd/listener.py`

```text
listener.py run|status|progress|watch|enable|disable --agent kimi|claude
    [--db bridge.db] [--config systemd/config.json] [--runtime-dir systemd/runtime]
    [--on-message systemd/on_message.py] [--once]
```

- `run` — сам цикл слушателя (обычно запускается autostart'ом, не вручную): flock-синглтон → pidfile → progress-документ → раз в ~1 с скан bridge.db на одно диспатчабельное письмо → `on_message.py` → ожидание выхода супервайзора → снова скан. Истёкшие активные диалоги идемпотентно уходят в `needs_human`. `AGENT_BRIDGE_AUTO_LISTEN=0` запрещает autostart при загрузке MCP.
- `status` — JSON `status()`: поля `agent, enabled, running, pid, message, auto, stage, progress, last`. Стадии: `stopped` (pid не подтверждён /proc), `unknown` (живой pid, но progress-документ отсутствует/не доверен), `idle`, `received`, `supervising`. `progress` — доверенный in-flight сегмент; `last` — исход последней диспетчеризации (показывается даже после краха). Доверие к документу — по равенству `listener_pid` pid из pidfile, проверенного через /proc.
- `progress` — одна строка, exit 0 всегда (для statusline).
- `watch` — та же строка раз в 1 с; `--once` — один снимок.
- `enable` / `disable` — см. MCP `listener`.

Формат `runtime/<agent>.progress.json` (пишется атомарным replace, только ids/enums/numbers): `listener_pid, updated_at, current {message, stage, supervisor_pid, since}, last {message, stage, result, supervisor_rc, duration_s}`. В работе (P0-3): разведение `unknown` на `legacy` (старый код listener'а), `starting` (файл ещё не создан), `error` (ошибка чтения) + аддитивный `progress_state`.

## 3. CLI `systemd/on_message.py` — one-shot supervisor

```text
on_message.py [message_id] [--recover] [--auto]
    [--db ...] [--config ...] [--runtime-dir ...]
    [--prompt-template systemd/dispatcher_prompt.md]
    [--claude-launcher cclocal_headless.sh] [--kimi-launcher kimi_headless.sh]
    [--workspace <vibe_coding>] [--mcp-config <workspace>/.mcp.json]
    [--worker-timeout 3300]
```

Единственный lifecycle-исполнитель сообщения: валидация → вставка строки `dispatch_attempts` (PK `message_id` — барьер «ровно одна попытка», авто-retry никогда) → сборка prompt'а из шаблона → выбор лаунчера по `recipient` → запуск воркера (отдельный process group) → классификация исхода **по evidence** (подтверждённый оригинал + результат по ключу `<prefix>:<id>:result`; саморепорт `<prefix>:<id>:blocked` принимается как честный blocked), а не по exit-коду → при неудаче короткий notice без payload инициатору. Жёсткий сбой (failed/timeout/interrupted/uncertain) замораживает активный диалог в `blocked`. Таймаут воркера 3300 с (под 60-минутным claim lease).

- `--auto` — автоматический режим: гейт `config.json auto.<recipient>` + диспатчатся только корневые задачи (legacy, `reply_to IS NULL) и `task`/`result` активных неистёкших диалогов; `notice` никогда никого не будит.
- без `--auto` — ручной запуск (это и есть «решение человека»), гейты обходятся сознательно.
- `--recover` — разбор зависших состояний: только письма без строки попытки; неопределённые исходы классифицируются по pid/result-уликам, ничего не перезапускается автоматически.

Состояния попытки (`dispatch_attempts.state`): `spawned, succeeded, blocked, failed, timeout, interrupted, uncertain`. Notice-уведомления отправляются при `failed/timeout/interrupted/uncertain`. История в prompt: до 12 сообщений на сторону, обрезка 1200 симв./письмо, потолок контекста 24000 симв., goal ≤ 4000 симв.

## 4. Лаунчеры воркеров

- `systemd/cclocal_headless.sh` — неинтерактивный Claude Code: собирает окружение из `~/.bash_aliases` (whitelist-eval только `LOCAL_CLAUDE_*`), токен строго через env ребёнка (не печатается, не логируется, не в БД), передаёт `ANTHROPIC_*`, `CLAUDE_CONFIG_DIR`, `NO_PROXY`. Переопределения для тестов: `CCLOCAL_ENV_FILE`, `CCLOCAL_CLAUDE_BIN`. Diagnostic-строку CLI `[claude-code:unrecognized_model]` (имя модели с локального endpoint нет в таблице алиасов CLI) лаунчер по умолчанию вырезает из потока, который supervisor пишет в `worker_<id>.out`; имя модели в запросе не меняется, exit code claude пробрасывается. Отключение: `CCLOCAL_STRIP_UNRECOGNIZED_MODEL=0`.
- `systemd/kimi_headless.sh` — официальный Kimi CLI в prompt-режиме: текст задачи передаётся через приватный request-файл 0600, **never argv**; `KIMI_CODE_TRUST_WORKSPACE=1`. Переопределение: `KIMI_BIN`. Денежный запрет: у Kimi `-p` нет интерактивных approval-промптов, поэтому `auto.kimi=false` по умолчанию — статические deny-правила не ослабляются.

Выбор лаунчера — по `message.recipient` (`AGENT_PREFIX`: claude → `cclocal`, kimi → `kimi`).

## 5. Шаблон `systemd/dispatcher_prompt.md`

Промпт воркера: плейсхолдеры `{{WORKER_LABEL}}, {{PERMISSION_NOTE}}, {{MESSAGE_ID}}, {{SENDER}}, {{REPLY_TO}}, {{CREATED_AT}}, {{RESPONSE_DEADLINE}}, {{CONTEXT}}, {{TEXT}}, {{PROTOCOL}}`. Протокол требует ровно один терминальный исход (result+ack / follow-up task+ack / complete / needs_human); внешние и необратимые действия воркер за другого не авторизует.

## 6. Состояние: БД и runtime

`bridge.db` (SQLite, WAL, busy_timeout):

| Таблица | Назначение |
|---|---|
| `messages` | id, sender, recipient, text, reply_to, created_at, acknowledged, response_deadline, dedup_key, claimed_by, claim_expires_at, conversation_id, kind, turn. Индексы: unread, уникальный dedup `(sender, dedup_key) WHERE dedup_key IS NOT NULL`, conversation. |
| `conversations` | id, goal, initiator, recipient, state (active/completed/blocked/needs_human/cancelled/expired), turn_count, max_turns, created_at, expires_at, updated_at. |
| `conversation_transitions` | аудит-лог переходов: conversation_id, at, state, reason. |
| `dispatch_attempts` | владение диспетчера: message_id PK, state, started_at, finished_at, pid, exit_code, error_kind. |

`systemd/schema.py` — аддитивная миграция `dispatch_attempts` (CREATE IF NOT EXISTS, ничего не ломает живую БД) + guard «это не bridge-база» (sqlite создаёт файл при connect — проверка наличия `messages`). Загружается через `importlib.util.spec_from_file_location`, не через `import schema`.

`systemd/runtime/` (права 0600, каталог 0700, gitignored):

| Файл | Содержимое |
|---|---|
| `<agent>.listener.pid` | pid listener'а (удаляется при штатном выходе) |
| `<agent>.listener.state` | `idle` или `waiting_worker:<message_id>` |
| `<agent>.listener.lock` | flock-синглтон |
| `<agent>.disabled` | маркер disable → graceful stop |
| `<agent>.progress.json` | текущая стадия + исход последней диспетчеризации (см. §2) |
| `dispatcher.log` | операционный лог слушателя/супервайзора, ids/enums/numbers only, 0600, ротация 1 МБ |
| `worker_<id>.out` | stdout воркера, 0600, keep-newest 20 |

## 7. Конфигурация и окружение

- `systemd/config.json` — гейт `auto`: `{"auto": {"claude": true, "kimi": false, "local": true}}`; listener перечитывает каждый скан. `auto.kimi` осознанно `false` (см. §4).
- `.mcp.json` (проектный, gitignored) — регистрация MCP-сервера: `uv run --directory <bridge> python server.py` + env `AGENT_ID`.
- `AGENT_ID` ∈ `{claude, kimi, local}` — личность процесса; без него инструменты отказывают.
- `AGENT_BRIDGE_AUTO_LISTEN=0` — запрет autostart listener'а при загрузке MCP (на явный `enable` не влияет).
- `AGENT_BRIDGE_DIR` — каталог моста (где `server.py`); подставляется в `systemd/mcp.local.json` как `${AGENT_BRIDGE_DIR}` — OpenClaude раскрывает переменные в `command`/`args`/`env` на своей стороне, дефолта нет, при отсутствии переменной подключение падает с явной ошибкой. `oclocal_headless.sh` экспортирует её сам (по своему месту в репо); интерактивный запуск требует export заранее.
- Statusline (опционально, только интерактивные сессии cclocal): `~/.claude-local/statusline-agent-bridge.sh` → `listener.py progress --agent claude --once`, `refreshInterval=2`. Настраивается вручную, bridge сам глобальные настройки не трогает.

## 8. Тесты

- `uv run python systemd/test_dispatcher.py` — безопасный набор (48 тестов): реальные `listener.py`/`on_message.py` против временных БД и фейковых лаунчеров/воркеров. Плоский скрипт без pytest: реестр `tests`, финал `ALL PASSED (N tests)`. Принятие — два зелёных прогона подряд. Одиночный тест:
  `uv run python -c "import sys; sys.path.insert(0,'systemd'); import test_dispatcher as t; t.run_test(t.test_NN_name); sys.exit(1 if t.FAILURES else 0)"`
- `test_bridge.py` — **деструктивный** E2E: удаляет и пересоздаёт `bridge.db`. Только в одноразовой копии репозитория, никогда на живой базе.

## 9. Инварианты и эксплуатация

- Логи, status/progress-файлы и worker-вывод содержат только ids, стадии, enum'ы и числа — никогда текст задачи, prompt'ы, вывод воркера и секреты (это проверяют SENTINEL-тесты).
- Автоматических повторов нет: краш/таймаут/uncertain остаётся видимым для решения человека.
- Барьер одной попытки: PK `dispatch_attempts.message_id`; рескан/рестарт/дубликат listener'а не запустят сообщение дважды.
- Терминальные диалоги не принимают новые `task`/`result`; жёсткий сбой цепочки замораживает её в `blocked`.
- Рестарт listener'я — только внешняя операция `disable` (маркер → graceful exit) → `enable`; из диспетчеризованного воркера это архитектурно невозможно.
- Работа с живым мостом: перед ручным takeover письма проверить отсутствие гонки с headless-воркером (`worker_<id>.out`, `<agent>.progress.json`, `SELECT * FROM dispatch_attempts WHERE message_id=N`); строка `state=blocked/uncertain` + непрочитанное письмо = воркер сдался, письмо свободно для claim.
- `response_deadline` — advisory-метаданные: мост ничего не пересылает автоматически, поздние ответы валидны.
- Не коммитить: `bridge.db`, `systemd/runtime/`, `outbox/`, `errors.log`, `.mcp.json`.
