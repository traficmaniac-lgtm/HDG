# State Machine

## Состояния
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

## Переходы
- `IDLE → CONNECTED`: успешный CONNECT.
- `CONNECTED → RUNNING`: START и успешное размещение BUY.
- `RUNNING → STOPPING`: STOP.
- `STOPPING → CONNECTED`: SELL завершён и ордера очищены.
- `CONNECTED → IDLE`: DISCONNECT.
- `ANY → ERROR`: критическая ошибка при работе сервисов/REST.
- `ERROR → CONNECTED/IDLE`: ручной DISCONNECT/CONNECT.

## Ограничения
- Параллельные торговые действия запрещены (single-flight).
- START недоступен без валидных API ключей и доступа к Margin API.

## Дополнение: TradeExecutor FSM (v0.7.2)
- SELL выставляется только из `POSITION_OPEN`.
- `WAIT_SELL` ждёт только `SELL_FILLED` (polling), без дополнительных auto-решений.
- STOP/ABORT отменяют активные ордера и возвращают цикл в `IDLE`.
