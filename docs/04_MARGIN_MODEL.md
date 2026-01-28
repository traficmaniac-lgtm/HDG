# Margin Model

## Cross Margin (единственный режим)
- Используется только Cross Margin (`margin_isolated = False`).
- `account_mode` хранится в настройках, но реальная работа идёт в Cross Margin.

## sideEffectType
- BUY-ордер может содержать `sideEffectType`:
  - `AUTO_BORROW_REPAY`, `MARGIN_BUY`, `NONE` (в UI задаётся как `side_effect_type`).
- SELL-ордер использует `AUTO_REPAY`, если `allow_borrow = True` и API разрешает borrow.
- Явные вызовы `borrow/repay` в торговом цикле не используются.

## BORROW: OK / FAIL
- `BORROW = OK|FAIL` отражает результат `probe_margin_borrow_access`.
- `FAIL` фиксируется при HTTP 401 или коде `-1002`.
- `OK` означает доступность borrow-эндпоинта, но не гарантирует фактический займ.

## Почему явный borrow НЕ используется в цикле
- Текущий цикл полагается на `sideEffectType` в ордерах.
- Явный borrow/repay применяется только как вспомогательные методы, не в основном цикле.
