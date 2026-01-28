# Технические характеристики торгового алгоритма

## Режим торговли и рынок

- Режим: **Cross Margin** (Binance).
- Инструмент: `BTCUSDT` по умолчанию (можно менять через UI).
- Стратегия: **Directional Hedge Scalping**.
- Вход: одновременный BUY и SELL одинакового объёма.

## Состояния конечного автомата

- `IDLE` → ожидание.
- `DETECT` → готов к запуску цикла (armed).
- `ENTERED_LONG` → заполнена длинная нога.
- `ENTERED_SHORT` → заполнена короткая нога.
- `WAIT_WINNER` → детект + ride победителя.
- `EXIT` → выход победителя.
- `FLATTEN` → управляемое/аварийное выравнивание.
- `COOLDOWN` → пауза между циклами.
- `ERROR` → ошибка и остановка цикла.

## Источники данных и выбор effective tick

- Источники:
  - **WebSocket** (`ws_market.py`): `bookTicker` + `depth10@100ms`.
  - **HTTP fallback** (`http_fallback.py`): `GET /api/v3/ticker/bookTicker`.
- Режимы: `WS_ONLY`, `HTTP_ONLY`, `HYBRID`.

Параметры переключения (`MarketDataService`):

- WS «свежий», если `ws_age_ms <= 500`.
- HTTP «свежий», если `http_age_ms <= 1200`.
- При падении WS удерживается HTTP ещё `1500ms`.
- Возврат на WS требует `ws_recovery_ticks = 3` подряд свежих тиков.

## REST и синхронизация времени

- Запросы к приватным эндпоинтам подписываются HMAC SHA256.
- `TimeSync` синхронизирует offset времени через `GET /api/v3/time`.
- При ошибке `-1021` происходит повторная синхронизация.

## Ордерная логика

- Типы ордеров:
  - `market` — немедленное исполнение (ожидание до `wait_fill_timeout_ms`).
  - `aggressive_limit` — лимит с `slip_bps`, ожидание `0.4s`, затем cancel + market.
- Вход:
  - SELL с `AUTO_BORROW_REPAY`.
  - BUY без side-effect.
- Закрытие:
  - Для шорта — `AUTO_REPAY`.
  - Для лонга — обычный SELL.

## Параметры стратегии (ключевые)

- `usd_notional` — номинал входа в USD.
- `max_spread_bps` — лимит спреда.
- `min_tick_rate` — минимальная скорость тиков.
- `use_impulse_filter` / `impulse_min_bps` — импульсный фильтр.
- `impulse_grace_ms` / `impulse_degrade_mode` — деградация импульса.
- `winner_threshold_bps` — порог определения победителя.
- `target_net_bps` — целевая прибыль после комиссий.
- `emergency_stop_bps` — экстренный стоп.
- `max_loss_bps` — стоп-лосс.
- `detect_timeout_ms` — таймаут детекта.
- `detect_window_ticks` — размер окна детекта (min 5).
- `cooldown_s` — пауза между циклами.
- `order_mode` / `slip_bps` — режим и проскальзывание.
- `allow_no_winner_flatten` / `no_winner_policy` — политика no-winner.
- `max_cycles` / `auto_loop` — автоцикл и лимит циклов.
- `wait_fill_timeout_ms` / `wait_exit_timeout_ms` — ожидания заполнения.
- `wait_positions_timeout_ms` — ожидание флет-позиции.
- `max_ride_ms` — лимит ride фазы.
- `data_stale_exit_ms` — выход при устаревании данных.

## Контроль риска и безопасности

- Запрет на вход при открытой экспозиции или активных ордерах.
- Проверка минимального лота и ноционала по фильтрам биржи.
- Ограничение по `leverage_max` относительно equity.
- Controlled/emergency flatten при сбоях исполнения.
- Выход при устаревании данных (`effective_age_ms > data_stale_exit_ms`).
- Защита от повторного входа при активном цикле.

## Производительность и надёжность

- `TradeEngine` работает в отдельном потоке от UI.
- Watchdog проверяет heartbeat UI каждые 2 секунды.
- Логи выводятся в UI и пишутся в файл (rotating).

## Журналы и мониторинг

- **CYCLE** — начало/конец цикла.
- **TRADE** — ключевые события входа/детекта/выхода.
- **STATE** — переходы состояний.
- **DEALS** — подтверждения ордеров и summary.
- **INFO/ERROR** — системные сообщения.
