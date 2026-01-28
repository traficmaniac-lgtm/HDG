# Market Data

## Источники
- **WS**: `wss://stream.binance.com:9443/ws/{symbol}@bookTicker`.
- **HTTP fallback**: `https://api.binance.com/api/v3/ticker/bookTicker`.

## Возраст и источник
- `ws_age_ms` и `http_age_ms` — возраст последнего апдейта для каждого источника.
- `source`: `WS`, `HTTP`, `NONE`.
- `mid_age_ms` — возраст последней рассчитанной mid цены.
- `last_switch_reason`: `WS fresh`, `HTTP fallback`, `No fresh data`.

## Правила валидности котировок
Котировка валидна, если:
- **WS**: `ws_age_ms <= ws_fresh_ms` и есть bid/ask.
- **HTTP**: `http_age_ms <= http_fresh_ms` и есть bid/ask.
- иначе `source = NONE`.

## Mid price
- `mid = (bid + ask) / 2`, если выбранный источник валиден и есть bid/ask.
- `mid = None`, если нет свежих данных или bid/ask отсутствуют.

## Дополнительно
- `ws_connected` — статус WS-соединения из воркера, отображается отдельно от свежести данных.
