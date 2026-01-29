# State Machine

## Состояния приложения
- **IDLE**: приложение запущено, подключения нет.
- **CONNECTED**: сервисы данных активны, торговля не запущена.
- **RUNNING**: активный торговый поток (есть BUY/SELL ордера или позиция).
- **STOPPING**: инициировано закрытие позиции/ордеров (STOP нажат).
- **ERROR**: критическая ошибка (например, REST/WS сбой), требует вмешательства оператора.

## Что разрешено в каждом состоянии
- **IDLE**: только CONNECT.
- **CONNECTED**: START, изменение настроек, наблюдение данных.
- **RUNNING**: только STOP, наблюдение данных.
- **STOPPING**: ожидание завершения закрытия и возврат в CONNECTED.
- **ERROR**: только DISCONNECT/CONNECT (ручная попытка восстановления).

## Переходы приложения
- `IDLE → CONNECTED`: успешный CONNECT.
- `CONNECTED → RUNNING`: START и успешное размещение BUY.
- `RUNNING → STOPPING`: STOP.
- `STOPPING → CONNECTED`: SELL завершён и ордера очищены.
- `CONNECTED → IDLE`: DISCONNECT.
- `ANY → ERROR`: критическая ошибка при работе сервисов/REST.
- `ERROR → CONNECTED/IDLE`: ручной DISCONNECT/CONNECT.

## TradeExecutor FSM (внутренний цикл)
- **STATE_IDLE** → **STATE_PLACE_BUY** → **STATE_WAIT_BUY** → **STATE_POSITION_OPEN**.
- **STATE_POSITION_OPEN** → **STATE_PLACE_SELL** → **STATE_WAIT_SELL** → **STATE_CLOSED** → **STATE_IDLE**.
- Ошибка размещения → **STATE_ERROR** → **STATE_CLOSED** → **STATE_IDLE**.

## Условия выхода (SELL)
- **Manual**: `close_position()` с причиной `MANUAL_CLOSE`.
- **TP/SL**: `evaluate_exit_conditions()` при достижении `mid`.
- **Emergency**: MARKET SELL только если остаток позиции слишком мал и лимитный SELL невозможен.

## Ограничения
- Параллельные торговые действия запрещены (single-flight).
- START недоступен без валидных API ключей и доступа к Margin API.
- SELL блокируется при отсутствии доступного баланса базового актива.
