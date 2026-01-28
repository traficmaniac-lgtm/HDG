# 02_ARCHITECTURE — Архитектура приложения

## Общая архитектура (уровни)
```
GUI (PySide6)
  ├─ управление (CONNECT / START / STOP / CLOSE)
  ├─ отображение данных
  └─ журналирование

Data Layer
  ├─ WS: bookTicker stream (binance)
  ├─ HTTP: bookTicker polling
  └─ PriceRouter (агрегация + устойчивость)

Trade Engine
  ├─ TradeExecutor (размещение/сопровождение ордеров)
  ├─ OrderTracker (поллинг статусов)
  └─ BinanceRestClient (REST + signed SAPI)

FSM (состояния торгового цикла)
  ├─ текущая FSM в TradeExecutor
  └─ целевая FSM v0.8.0 (строгое управление переходами)
```

## Data Layer
### WS (WebSocket)
- Подключение к `wss://stream.binance.com:9443/ws/{symbol}@bookTicker`.
- Получение bid/ask по каждому сообщению.
- Защита от зависаний:
  - `TimeoutError` если нет первого тика > 3 сек.
  - `TimeoutError` если тики «замерли» > 2 сек.
  - Автопереподключение с экспоненциальным backoff.

### HTTP (REST polling)
- Запрос `GET /api/v3/ticker/bookTicker`.
- Используется как резерв при проблемах WS или по правилам data-health.

### PriceRouter
- Сводит WS и HTTP, выдаёт **стабильную цену**.
- Поддерживает:
  - определение свежести данных (ws/http age),
  - переключение источников с гистерезисом,
  - `last_good_quote` (кэш последней валидной цены),
  - метки data_blind / from_cache / ttl_expired.

## Trade Engine
### TradeExecutor
- Главный торговый движок.
- Отвечает за:
  - размещение BUY и SELL ордеров,
  - учёт состояния позиции,
  - расчёт PnL,
  - валидацию параметров (tick/step/minQty).

### OrderTracker
- Периодически опрашивает REST API по каждому активному ордеру.
- Обновляет статусы (NEW → FILLED / CANCELED / REJECTED / EXPIRED).
- Триггерит обработчики в TradeExecutor.

### BinanceRestClient
- Единая точка для REST/SAPI запросов.
- Поддерживает подпись, API key/secret и calls к margin API.

## FSM (состояния)
- Текущая FSM реализована в `TradeExecutor` (IDLE → PLACE_BUY → WAIT_BUY → POSITION_OPEN → PLACE_SELL → WAIT_SELL → CLOSED/ERROR).
- **v0.8.0** требует унифицированную FSM с чёткими запрещёнными переходами (см. 07_STATE_MACHINE).

## Стабильные части
- Data Layer (WS + HTTP + Router).
- Основные REST операции (order/margin account/open orders).
- GUI отображение состояния и логов.

## Нестабильные/в разработке
- Полная FSM уровня стратегии (ARMED → EXIT → COOLDOWN).
- Гарантированная обработка «частичных» ордеров и edge-case статусов.
- Централизованная политика аварий (data_blind, borrow fail, stale orders).
