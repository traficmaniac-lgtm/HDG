# Margin Model

## Только Cross Margin
- Используется только **Cross Margin** (isolated не применяется).
- Плечо: **x3**.

## sideEffectType
- **BUY** может использовать:
  - `AUTO_BORROW_REPAY`
  - `MARGIN_BUY`
  - `NONE`
- **SELL** использует `AUTO_REPAY`, когда разрешён borrow и это поддерживается API (независимо от LIMIT/MARKET выхода).

## BORROW: OK / FAIL
- `BORROW = OK|FAIL` отражает результат `probe_margin_borrow_access`.
- `FAIL` фиксируется при HTTP 401 или коде `-1002`.
- `OK` означает доступность borrow-эндпоинта, но не гарантию фактического займа.

## Почему явный borrow не используется в цикле
- Цикл опирается на `sideEffectType` в ордерах.
- Явные `borrow/repay` — только вспомогательные методы вне основного цикла.
