# Order Lifecycle

## Жизненный цикл ордера
1. **Intention**: GUI вызывает `place_test_orders_margin()` или `close_position()`.
2. **NEW**: ордер добавляется в `active_test_orders` со статусом `NEW`.
3. **FILLED / CANCELED / EXPIRED**:
   - `sync_open_orders()` сравнивает локальный список с `openOrders` Binance.
   - Если ордера нет в `openOrders`, он помечается как `FILLED` локально.

## Разница между intention и фактом на бирже
- Intention — это локальное действие (кнопка START/STOP).
- Факт — статус на бирже (`openOrders`/SAPI ответ).
- В текущей реализации отсутствует отдельная обработка `CANCELED`/`EXPIRED` от биржи.

## Ограничения текущей реализации
- Нет явной обработки частичных исполнений.
- Нет подтверждения статуса `CANCELED`/`EXPIRED` от биржи — отсутствие в `openOrders` трактуется как `FILLED`.
- Один активный тестовый поток (single-flight): параллельные торговые действия запрещены.
