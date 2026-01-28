# 08_GUI_CONTRACT — Контракт GUI

## Что GUI ПОКАЗЫВАЕТ
### Верхняя панель (summary)
- Символ (EURIUSDT).
- Источник цены (SRC: WS / HTTP / NONE).
- MARGIN_AUTH (OK/FAIL).
- BORROW (OK/FAIL/—).
- FSM состояние и last_action.
- Кол-во активных ордеров.

### Сводка (Terminal → Сводка)
- **Market**: Mid, Bid, Ask, SRC.
- **Health**: ws_connected, ws_age_ms, http_age_ms, last_switch_reason.
- **Profile**: tickSize, stepSize, minQty, minNotional.
- **Position**: buy_price, текущий mid, tp/sl триггеры.

### Ордера
- Таблица ордеров: время, orderId, side, price, qty, status, age_ms, pnl_est, tag.
- PnL секция: Unrealized, PnL за цикл, Session PnL, Last exit.

### Logs
- Все технические события и ошибки (data_blind, WS reconnect, margin check и т.д.).

## Что GUI НЕ ДЕЛАЕТ
- Не принимает торговые решения (нет стратегии в GUI).
- Не выполняет оптимизацию параметров.
- Не производит автоматическую смену символа.
- Не управляет риском за пределами TP/SL и аварийных блокировок.
- Не выполняет бэктест/симуляцию.

## Какие данные обязательны
- Цена (bid/ask/mid) и источник.
- Статусы WS/HTTP (health).
- Tick/Step/MinQty/MinNotional профиль.
- Состояние ордеров и позиция.
- PnL (cycle/session).

## Какие элементы уже зафиксированы и не меняются
- Общая структура GUI (две вкладки: Terminal, Logs).
- Summary строка и набор полей в ней.
- Таблица ордеров и PnL панель.
- Статусы WS/HTTP и логика отображения.
