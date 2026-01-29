# Order Lifecycle

## Жизненный цикл ордера
1. **Intention**: GUI вызывает `place_test_orders_margin()` или `close_position()`.
2. **PRECHECK**: проверяются котировки, тик/шаг, minQty/minNotional, бюджет и балансы.
3. **PLACE**: лимитный BUY или SELL отправляется на биржу через REST.
4. **WAIT**: ордер отслеживается локально и синхронизируется через `openOrders`.
5. **FILLED/CLOSED**: по заполнению обновляются позиции и PnL.

## Разница между intention и фактом на бирже
- Intention — это локальное действие (кнопка START/STOP).
- Факт — статус на бирже (`openOrders` + события частичного/полного заполнения).
- В текущей реализации отсутствие ордера в `openOrders` трактуется как `FILLED`.

## BUY: правила выставления
- **Тип**: только LIMIT, `MARKET` запрещён.
- **Цена**: рассчитывается от `bid` и округляется по `tick_size`.
- **Количество**: `order_quote / buy_price`, округление по `step_size`.
- **Ограничения**: `minQty`, `minNotional`, `budget_reserve`, `max_budget`.
- **sideEffectType**: из настроек (`AUTO_BORROW_REPAY`, `MARGIN_BUY`, `NONE`).

## SELL: правила выставления
- **Тип**: по `exit_order_type` (LIMIT, либо MARKET только в emergency).
- **Цена LIMIT**: от `ask` с `exit_offset_ticks` и округлением по `tick_size`.
- **Количество**: оставшийся `qty` позиции, округление по `step_size`.
- **Баланс**: проверяется `free/locked` базового актива, чтобы избежать SELL без доступного баланса.
- **sideEffectType**: `AUTO_REPAY`, если borrow разрешён.

## Локальное хранилище ордеров
`active_test_orders` содержит поля:
- `orderId`, `side`, `price`, `qty`, `status`, `clientOrderId`.
- Для SELL дополнительно `sell_status`, `reason`.
- Служебные отметки `created_ts`, `updated_ts`, `cum_qty`, `avg_fill_price`.

## Частичные исполнения
- BUY/SELL partial обновляют `cum_qty`/`avg_fill_price`.
- Для SELL агрегируется `sell_executed_qty`, пересчитывается остаток позиции.
- Если остаток меньше допуска (`step_size tolerance`) — позиция закрывается как `PARTIAL`.

## Ограничения текущей реализации
- Нет явной обработки `CANCELED`/`EXPIRED` от биржи — отсутствие в `openOrders` трактуется как `FILLED`.
- Один активный тестовый поток (single-flight): параллельные торговые действия запрещены.
- В одном цикле допускается только один активный SELL; авто-выходы TP/SL не выставляют повторные ордера.
