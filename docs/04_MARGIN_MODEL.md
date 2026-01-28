# Margin Model

## Cross Margin
- Режим по умолчанию: Cross Margin (`margin_isolated = False`).
- `account_mode` хранится в настройках, но фактическая работа — Cross Margin.

## Borrow / Auto-Repay
- BUY-ордер может содержать `sideEffectType`:
  - `AUTO_BORROW_REPAY`, `MARGIN_BUY`, `NONE`.
- SELL-ордер использует `AUTO_REPAY`, если `allow_borrow = True` и API разрешает borrow.
- Явный вызов borrow/repay в текущем цикле не используется (только sideEffectType).

## BORROW: FAIL / OK
- Статус `BORROW` отражает результат проверки `probe_margin_borrow_access`.
- `FAIL` возможен при HTTP 401 или коде `-1002` (нет прав на borrow).
- `OK` означает, что borrow-эндпоинт доступен (не гарантирует фактический займ).

## Допустимые режимы
- `margin_isolated`: `False` (Cross Margin). True не используется в текущем UI-флоу.
- Типы ордеров: BUY = `LIMIT` или `MARKET`, SELL = `LIMIT`.
