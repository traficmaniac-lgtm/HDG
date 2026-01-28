# State Machine

## Состояния
- **IDLE**: нет активных ордеров, позиция отсутствует.
- **BUY_PLACED**: BUY ордер создан, статус `NEW`.
- **POSITION_OPEN**: BUY FILLED, `position` заполнена.
- **CLOSING**: SELL ордер создан для закрытия.
- **CLOSED**: SELL FILLED, позиция очищена, PnL зафиксирован.

## Допустимые переходы
- `IDLE → BUY_PLACED` (START).
- `BUY_PLACED → POSITION_OPEN` (BUY FILLED).
- `BUY_PLACED → CLOSING` (STOP до заполнения BUY).
- `POSITION_OPEN → CLOSING` (STOP).
- `CLOSING → CLOSED` (SELL FILLED).
- `CLOSED → IDLE` (очистка активных ордеров, ожидание следующего цикла).

## Чего делать нельзя
- START при активном BUY или открытой позиции.
- STOP при отсутствии BUY/позиции.
- Параллельные торговые действия (enforced single-flight).
