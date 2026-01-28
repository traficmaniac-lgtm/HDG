# Architecture

## Модули
- **core**: модели данных (Settings, PriceState, HealthState, SymbolProfile), версия, хранение настроек.
- **services**:
  - WsPriceWorker — WS bookTicker поток.
  - HttpPriceService — HTTP bookTicker fallback.
  - PriceRouter — выбор «лучшей» котировки и health-метрик.
  - BinanceRestClient — REST клиент Binance (Spot/Margin SAPI).
  - TradeExecutor — логика размещения/закрытия ордеров, позиция, PnL.
- **gui**: MainWindow и диалоги настроек.

## Связи
- GUI создаёт PriceRouter, REST и TradeExecutor.
- WsPriceWorker/HttpPriceService обновляют PriceRouter.
- TradeExecutor использует PriceRouter (mid) и BinanceRestClient (ордеры/аккаунт).

## Data Flow (market → order → position)
1. **Market**: WS/HTTP → PriceRouter → PriceState/HealthState → GUI.
2. **Order**: GUI → TradeExecutor.place_test_orders_margin → REST create_margin_order → open orders.
3. **Position**: TradeExecutor.sync_open_orders → BUY FILLED → position создана → GUI отображает.
4. **Close**: GUI → TradeExecutor.close_position → SELL → SELL FILLED → position закрыта → PnL обновлён.
