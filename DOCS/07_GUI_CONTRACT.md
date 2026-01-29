# GUI Contract

## Что GUI МОЖЕТ делать
- Подключать/отключать сервисы данных (CONNECT/DISCONNECT).
- Инициировать торговые действия: START (старт цикла), STOP (остановка цикла), CLOSE (ручной выход).
- Открывать/сохранять настройки API и параметров торговли.
- Отображать котировки, статусы, состояние сессии, PnL.

## Что GUI НЕ МОЖЕТ делать
- Принимать решения о входе/выходе или стратегии.
- Изменять правила маршрутизации котировок.
- Форсировать торговые действия в обход TradeExecutor.
- Обходить проверки доступа к Margin API.

## Какие данные GUI только отображает
- `PriceState`, `HealthState`, `SymbolProfile`.
- Снимок активных ордеров и их статусы/возраст.
- `pnl_cycle`, `pnl_session`, `unrealized`.
- `buy_price`, `tp_trigger_price`, `sl_trigger_price`, `mid`, `last_exit_reason`.
- Статусы `MARGIN_AUTH` и `BORROW`.

## Какие действия GUI лишь инициирует
- START/STOP/CLOSE — вызовы методов `TradeExecutor`.
- CONNECT/DISCONNECT — запуск/остановка сервисов WS/HTTP/REST/OrderTracker.
- Сохранение настроек — обновление `Settings`, без прямого изменения логики исполнения.
