# GUI Contract

## Что GUI МОЖЕТ делать
- Подключать/отключать сервисы данных (CONNECT/DISCONNECT).
- Инициировать торговые действия: START (BUY), STOP (SELL).
- Открывать/сохранять настройки API и параметров торговли.
- Отображать котировки, статусы и PnL.

## Что GUI НЕ МОЖЕТ делать
- Принимать решения о входе/выходе или стратегии.
- Изменять правила маршрутизации котировок.
- Выполнять ручные borrow/repay вне `TradeExecutor`.
- Обходить проверки доступа к Margin API.

## Какие данные GUI только отображает
- `PriceState`, `HealthState`, `SymbolProfile`.
- Снимок активных ордеров и статусы исполнения.
- `pnl_cycle`, `pnl_session`, `unrealized`.
- `buy_price`, `tp_trigger_price`, `sl_trigger_price`, `mid`, `last_exit_reason`.
- `MARGIN_AUTH` и `BORROW` статусы.

## Какие действия GUI лишь инициирует
- START/STOP — только запуск функций `TradeExecutor`.
- CONNECT/DISCONNECT — только запуск/остановка сервисов.
