# GUI Contract

## Какие данные GUI получает
- PriceState: bid/ask/mid, source, mid_age_ms.
- HealthState: ws_connected, ws_age_ms, http_age_ms, last_switch_reason.
- SymbolProfile: tickSize, stepSize, minQty, minNotional.
- Orders snapshot: orderId, side, price, qty, status, clientOrderId, timestamps.
- Trading state: last_action, orders_count, pnl_cycle, pnl_session.
- Margin state: MARGIN_AUTH, BORROW (OK/FAIL).

## Какие события GUI только отображает
- Статус соединений (WS/HTTP), ошибки REST, лог-сообщения.
- Состояние ордеров и позиции, PnL.
- Текущее состояние API (валидность ключей, разрешения).

## Что GUI НЕ ДОЛЖЕН делать
- Не принимать торговые решения (когда входить/выходить) — это зона TradeExecutor.
- Не менять логику маршрутизации цен (только отображение состояния).
- Не выполнять ручные borrow/repay операции вне TradeExecutor.
