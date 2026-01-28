# Architecture

## Общая архитектура модулей
- **GUI**: `MainWindow`, диалоги настроек, управление кнопками CONNECT/START/STOP.
- **Core**: модели данных (`Settings`, `PriceState`, `HealthState`, `SymbolProfile`), версия.
- **Services**:
  - `WsPriceWorker` — WS поток `bookTicker`.
  - `HttpPriceService` — HTTP `bookTicker` fallback.
  - `PriceRouter` — выбор лучшей котировки и расчёт метрик свежести.
  - `BinanceRestClient` — REST/SAPI доступ к Binance.
  - `TradeExecutor` — размещение/закрытие ордеров, позиция, PnL.
- **Binance**: внешние API (WS/HTTP/SAPI).

## Потоки данных
- **WS**: Binance WS → `WsPriceWorker` → `PriceRouter` → `PriceState`/`HealthState` → GUI.
- **HTTP**: GUI таймер → `HttpPriceService` → `PriceRouter` → `PriceState`/`HealthState` → GUI.
- **Orders**: GUI → `TradeExecutor` → `BinanceRestClient` → Binance SAPI → `sync_open_orders()` → GUI.

## Границы ответственности
- GUI инициирует действия и отображает состояние, но не принимает торговые решения.
- `PriceRouter` отвечает только за источник котировок и метрики свежести.
- `TradeExecutor` отвечает за торговые действия и расчёты PnL.
- `BinanceRestClient` — транспортный слой для REST/SAPI без бизнес-логики.
