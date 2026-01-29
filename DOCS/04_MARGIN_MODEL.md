# Margin Model

## Режим маржи
- Поддерживаются **Cross** и **Isolated** через флаг `margin_isolated`.
- В запросах Binance используется `isIsolated = TRUE|FALSE`.

## sideEffectType
### BUY
- Значение берётся из настройки `side_effect_type`.
- Нормализация:
  - `AUTO_BORROW_REPAY` или `MARGIN_BUY` — принимаются как есть.
  - `NONE` — не передаётся в запрос.
  - Любое другое значение → `AUTO_BORROW_REPAY`.

### SELL
- При `allow_borrow = True` и если API не запретил borrow, SELL использует `AUTO_REPAY`.
- Если borrow недоступен → `sideEffectType` не указывается.

## Проверка доступа к borrow
- GUI получает флаг `borrow_allowed_by_api` (через тестовый запрос к borrow‑эндпоинту).
- Ошибки `401` и `-1002` фиксируют запрет borrow и отключают `AUTO_REPAY` для SELL.

## Что НЕ делает текущая логика
- Нет явного borrow/repay в основном цикле — только sideEffectType.
- Explicit borrow/repay методы существуют как вспомогательные, но цикл ими не пользуется.
