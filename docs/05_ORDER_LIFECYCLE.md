# Order Lifecycle

## BUY / SELL
- START в GUI вызывает `place_test_orders_margin()` и размещает **BUY**.
- STOP вызывает `close_position()` и размещает **SELL** для закрытия.

## NEW / FILLED / CANCELED
- После размещения ордер создаётся как `NEW` в `active_test_orders`.
- `sync_open_orders()` сверяет с Binance openOrders:
  - если ордер отсутствует → помечается как `FILLED`.
- Отмена возможна через `cancel_test_orders_margin()` (отменяет открытые ордера и чистит список).

## Что считается «открытой позицией»
- Позиция считается открытой, когда `position` заполнена (после BUY FILLED).
- Пока BUY в статусе `NEW`, позиция ещё не открыта.

## Правила закрытия
- Закрытие возможно, если есть BUY в списке или `position` уже открыта.
- SELL размещается по цене `mid + offset_ticks * tick_size` и количеству позиции/BUY.
- После SELL FILLED позиция очищается, `pnl_cycle` фиксируется.
