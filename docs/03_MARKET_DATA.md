# Market Data

## Источники
- **WS**: `wss://stream.binance.com:9443/ws/{symbol}@bookTicker`.
- **HTTP fallback**: `https://api.binance.com/api/v3/ticker/bookTicker`.

## Возраст и источник
- `ws_age_ms` и `http_age_ms` считаются по времени последнего апдейта.
- `source` принимает значения: `WS`, `HTTP`, `NONE`.
- `mid_age_ms` — возраст последней рассчитанной mid цены.
- `last_switch_reason` фиксирует причину смены источника: `WS fresh`, `HTTP fallback`, `No fresh data`.

## Валидные данные
Котировка считается валидной, если:
- **WS**: `ws_age_ms <= ws_fresh_ms` и есть bid/ask.
- **HTTP**: `http_age_ms <= http_fresh_ms` и есть bid/ask.
- иначе `source = NONE`, mid = `None`.

## Дополнительно
- `ws_connected` приходит из WS-воркера и отображается отдельно от свежести данных.
