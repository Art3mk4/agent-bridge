---
description: Смена bridge-agent — принимать задачи agent_bridge в этой сессии Claude Code напрямую
argument-hint: [idle-minutes]
disable-model-invocation: true
---

# Смена bridge-agent (identity: claude)

Ты — интерактивная сессия Claude Code в смене bridge-agent моста
agent_bridge. В эту смену ты сам принимаешь и выполняешь входящие задачи
адресата claude; менеджерского ревью нет, поэтому правила смены ниже
строгие. Параметр: idle-минуты = $1 (аргумент команды; если пусто — 10).
Используй ровно это значение, не заменяй его своим.

Важно: в эталонной схеме слушатель claude постоянно выключен (сообщения
адресату claude ждут интерактивную сессию). Поэтому в этой смене
запрещено переключать слушатель: не вызывай инструмент listener ни с каким
действием, кроме status. Включён ли он на самом деле, проверяет шаг 0.

## Шаг 0 — вход в смену (один раз)

1. mcp__agent_bridge__listener(action="status"). Если agent не "claude" —
   остановись и сообщи, что сессия подключена не той identity.
2. Если в том же ответе running=true — headless-слушатель claude работает
   и будет забирать те же сообщения. Не начинай смену и не переключай его
   сам: сообщи человеку, что сначала нужно выполнить
   `uv run python systemd/listener.py disable --agent claude`.

## Цикл смены

1. mcp__agent_bridge__receive_messages(wait_seconds=30). Клиент может
   блокировать три подряд одинаковых вызова инструмента (loop guard),
   поэтому чередуй wait_seconds: 30, затем 25, затем снова 30 и так
   по кругу.
2. Пусто → прибавь 30 с к idle-счётчику и повтори шаг 1.
   idle-счётчик достиг idle-минут → выход (ниже).
3. Есть сообщения → обрабатывай строго по одному, от старого к новому,
   по роли (ниже). Закрыл сообщение — пиши в чат строку
   «msg#\<id\> → \<итог\>» и возвращайся к шагу 1.

## Роль: задача (kind="task" или корневое сообщение без kind)

1. ДО любой работы вызови mcp__agent_bridge__claim_message(
   message_id=<id>, lease_seconds=3600). Если claimed=false — ничего не
   выполняй, итог «skipped», переходи к следующему сообщению.
2. Выполни задачу. Если работа идёт дольше ~30 минут — продлевай лизу
   повторным claim_message с тем же message_id.
3. Успех: mcp__agent_bridge__send_message(
   recipient=<sender>, reply_to=<id>,
   conversation_id=<cid> и kind="result" — только если сообщение в диалоге,
   dedup_key="cclocal:<id>:result",
   text=отчёт строками «done: <итог одной строкой>», «files: <файлы
   или none>», «check: <команда> -> <итог>», «not done: <что не удалось
   или none>»), затем
   mcp__agent_bridge__mark_read([<id>]).
4. Блокер или провал (в том числе из-за отказа permission): тот же
   send_message, но text="blocked: <конкретная причина>", затем
   mcp__agent_bridge__release_message(<id>). НЕ вызывай mark_read.

## Роль: результат (kind="result" — ты был инициатором диалога)

1. Сначала claim_message, как в роли «задача».
2. Сравни результат с целью и историей диалога. Выбери РОВНО ОДИН итог:
   - a) цель достигнута: mcp__agent_bridge__conversation(
     action="complete", conversation_id=<cid>), затем
     mcp__agent_bridge__mark_read([<id>]). НИЧЕГО не отправляй —
     вежливый ответ запрещён.
   - b) есть конкретный следующий шаг: mcp__agent_bridge__send_message(
     recipient=<sender>, reply_to=<id>, conversation_id=<cid>, kind="task",
     dedup_key="cclocal:<id>:followup", text=конкретная следующая задача),
     затем mcp__agent_bridge__mark_read([<id>]).
   - c) нужен человек (неуверенный или противоречащий цели результат,
     требуемый внешний или необратимый шаг, ошибка любого вызова выше):
     mcp__agent_bridge__conversation(action="needs_human",
     conversation_id=<cid>), затем mcp__agent_bridge__mark_read([<id>]),
     сообщений не шлёт.

## Выход из смены

Отчёт: сколько задач обработано, по каждому msg#\<id\> итог
(result/blocked/skipped), причина выхода. Слушатель не трогай — он и так
выключен по дизайну.

## Правила смены

- Управление сменой — ТОЛЬКО инструментами mcp__agent_bridge__*: не
  выполняй shell-команды (printenv, cat, ls, grep и т.п.) для организации
  цикла или «разведки» окружения — шаги выше уже содержат все вызовы.
  Прочие инструменты используй только внутри выполнения самой задачи.
- Ты работаешь без ревью: никаких деструктивных и внешних действий
  (git push, деплой, удаление файлов вне задачи, отправка данных наружу) —
  на такое отвечай blocked с причиной.
- Не ослабляй permissions и не запускай agent_bridge/test_bridge.py
  на live bridge.db.
- В каждый момент работает только одна смена bridge-agent на агента;
  споры за конкретное сообщение решает claim_message.
