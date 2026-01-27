# Технические характеристики торгового алгоритма

## Режим торговли и рынок

- Режим: **Cross Margin** (Binance).
- Инструмент: `BTCUSDT` (по умолчанию в `BinanceMarginExecution`).
- Стратегия: **Directional Hedge Scalping**.
- Вход: одновременный BUY и SELL одинакового объёма.

## Состояния конечного автомата

- `IDLE` → ожидание.
- `ARMED` → готов к запуску цикла.
- `ENTERING` → вход и открытие хеджа.
- `DETECTING` → поиск победителя.
- `CUTTING` → закрытие проигравшей ноги.
- `RIDING` → сопровождение победителя.
- `EXITING` → выход победителя.
- `CONTROLLED_FLATTEN` → управляемое выравнивание.
- `COOLDOWN` → пауза перед следующим циклом.
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
  - `market` — немедленное исполнение (ожидание до 3s).
  - `aggressive_limit` — лимит с `slip_bps`, ожидание 0.4s, затем cancel + market.
- Вход:
  - SELL с `AUTO_BORROW_REPAY`.
  - BUY без side-effect.
- Закрытие:
  - Для шорта — `AUTO_REPAY`.
  - Для лонга — обычный SELL.
- No-winner:
  - `NO_LOSS` → IOC лимитные заявки, fallback на market.
  - `FLATTEN` → controlled flatten.

## Параметры стратегии (ключевые)

- `usd_notional` — номинал входа в USD.
- `max_spread_bps` — лимит спреда.
- `min_tick_rate` — минимальная скорость тиков.
- `use_impulse_filter` / `impulse_min_bps` — импульсный фильтр.
- `impulse_grace_ms` — время, после которого импульс может деградировать.
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

## Контроль риска и безопасности

- Запрет на вход при открытой экспозиции или активных ордерах.
- Проверка минимального лота и ноционала по фильтрам биржи.
- Ограничение по `leverage_max` относительно equity.
- Controlled/emergency flatten при сбоях исполнения.
- Выход при устаревании данных (`effective_age_ms > 1500`).

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
