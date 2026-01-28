# 06_PNL_MODEL — Расчёт PnL

## Как считается PnL
### Реализованный (realized)
- Фиксируется после полного исполнения SELL.
- Формула:
  - `PnL = (sell_price - buy_price) * qty`
- Валюта: **quote currency** (для EURIUSDT это USDT).

### Нереализованный (unrealized)
- Оценивается на основе текущего `mid`:
  - `PnL_unrealized = (mid - buy_price) * qty`.
- Если BUY ордер ещё не исполнен (NEW), используется цена BUY.

## ticks / bps / currency
- **ticks**: TP/SL выражены в количестве тиков (`take_profit_ticks`, `stop_loss_ticks`).
- **bps**: в логах вычисляется `pnl_bps = (pnl / notional) * 10000`, где `notional = buy_price * qty`.
- **currency**: все значения PnL в **USDT**.

## Требования к точности
- Все цены и объёмы должны соответствовать:
  - `tickSize` (цена),
  - `stepSize` (количество).
- Округление должно быть стабильным и однозначным (round/ceil/floor согласно правилам торгового движка).

## Логирование
- При закрытии SELL:
  - `[PNL] realized=...` или `[PNL] realized reason=TP/SL ...`.
- Параллельно отображается Session PnL в GUI.
- Любой расхождений PnL между GUI и логом быть не должно.
