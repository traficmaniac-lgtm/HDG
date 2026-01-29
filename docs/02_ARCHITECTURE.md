# Architecture

## Общая архитектура модулей
- **MarketData**: поток котировок из WS/HTTP, метрики свежести, выбор источника, расчёт `mid`.
- **PriceRouter**: унифицирует правила валидности и отдаёт `PriceState`/`HealthState` в TradeExecutor.
- **TradeExecutor**: жизненный цикл ордеров BUY/SELL, позиция, базовый PnL и авто-выход по TP/SL.
- **Margin**: настройки режима, sideEffectType, проверки доступности borrow.
- **GUI**: пользовательское управление (CONNECT/START/STOP) и отображение состояния.

## Границы ответственности модулей
- **MarketData/PriceRouter** отвечают только за данные и их здоровье (freshness/валидность). Торговые решения не принимают.
- **TradeExecutor** отвечает за размещение ордеров и переходы состояний, а также за журналирование причин блокировок.
- **Margin** задаёт режим счёта и политику `sideEffectType`.
- **GUI** инициирует действия и отображает данные, но не принимает решений.

## Поток данных (happy path)
1. `MarketData` получает `bid/ask` через WS, резерв — HTTP.
2. `PriceRouter` вычисляет `mid` и отдаёт `PriceState` + `HealthState`.
3. `TradeExecutor` валидирует `mid`, `bid/ask`, тик/шаг, minQty/minNotional, бюджет.
4. На успехе — размещает BUY, затем SELL, ведёт цикл и завершает с фиксацией PnL.

## Поток управления
- **GUI** вызывает `TradeExecutor.place_test_orders_margin()` (START) и `TradeExecutor.close_position()` (STOP).
- **TradeExecutor** вызывает `BinanceRestClient` для размещения/синхронизации.
- **TradeExecutor** инициирует авто-выход при TP/SL через `evaluate_exit_conditions()`.

## Основные контракты данных
- `PriceState`: `bid`, `ask`, `mid`, признак `data_blind`.
- `HealthState`: `ws_age_ms`, `http_age_ms`, `mid_age_ms` и политики свежести.
- `OrderEntry`: локальная запись ордера (`orderId`, `side`, `price`, `qty`, `status`, `clientOrderId`).
- `Position`: агрегированная позиция (BUY цена, qty, partial flags).

## Обработка ошибок
- Отсутствие цены → блокировка входа/выхода с явной причиной `NO_PRICE`.
- Некорректные лимиты биржи (tick/step/minQty) → отказ размещения.
- Ошибки Binance (например `-2010`) → точечная обработка и логирование.
- Критические ошибки → переход в `STATE_ERROR` и завершение цикла.
