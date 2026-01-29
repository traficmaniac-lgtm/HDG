# Architecture

## Общая архитектура модулей
- **GUI (MainWindow)**: кнопки CONNECT/START/STOP/CLOSE, отображение статусов, хранение пользовательских настроек.
- **Market Data**:
  - `WsPriceWorker` — подписка на `bookTicker` через WS.
  - `HttpPriceService` — резервный HTTP `bookTicker`.
  - `PriceRouter` — нормализует источники, выбирает «эффективный» источник, рассчитывает `mid`, возраст данных, кеширование и TTL.
- **TradeExecutor**: конечный автомат торгового цикла (BUY → SELL), контроль статусов, таймауты, авто‑выход, обработка частичных исполнений.
- **OrderTracker**: периодический REST‑polling каждого активного ордера и события `PARTIALLY_FILLED`/`FILLED`/`CANCELED`/`EXPIRED`.
- **BinanceRestClient**: единая точка REST‑вызовов (маржа, ордера, символы).

## Поток данных
1. **WS/HTTP котировки → PriceRouter**: обновление bid/ask, расчёт `mid`, источник данных и возраст.
2. **PriceRouter → GUI**: показ `PriceState`/`HealthState` (mid, age, source, data_blind).
3. **PriceRouter → TradeExecutor**: снапшоты цен для входа/выхода и анти‑stale проверок.
4. **TradeExecutor → OrderTracker**: поддержка списка активных ордеров.
5. **OrderTracker → TradeExecutor**: события частичных/полных исполнений и финальных статусов.

## Границы ответственности
- **PriceRouter** не принимает торговых решений — только маршрутизация и нормализация цен.
- **TradeExecutor** отвечает за детерминированный цикл, но не инициирует его без команды GUI.
- **OrderTracker** не содержит бизнес‑логики, только приводит статусы к событиям.
- **GUI** управляет сессией и отображением, но не вмешивается в автомат торгового цикла.

## Контроль состояния
- Состояние соединения (GUI) отделено от торгового состояния (TradeExecutor FSM).
- Торговый цикл управляется таймерами GUI (`_orders_timer`/`_ui_timer`) и событиями OrderTracker.
