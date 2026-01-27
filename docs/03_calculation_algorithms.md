# Алгоритмы расчётов (метрики и фильтры)

Этот документ описывает формулы, вычисления и пороговые проверки,
которые используются алгоритмом в разных фазах цикла.

## Базовые вычисления цены

- **Mid price**: `mid = (bid + ask) / 2` (или приходит напрямую в тике).
- **Spread в bps**: `spread_bps = (ask - bid) / mid * 10_000`.
- **Raw bps**:
  - LONG: `(to_mid - from_mid) / from_mid * 10_000`.
  - SHORT: `(from_mid - to_mid) / from_mid * 10_000`.

## Скорость тиков и импульс

- **Tick rate** — количество тиков за последнюю секунду:
  `tick_rate = ticks_last_1s / 1s`.
- **Импульс (impulse_bps)** — абсолютное изменение mid между двумя последними WS-тика:
  `abs(last_ws_mid - prev_ws_mid) / prev_ws_mid * 10_000`.

## Проверка свежести данных

Для вычислений берётся **effective tick** (WS/HTTP в зависимости от режима). Значение
`data_stale = True`, если источник данных `NONE`.

- **Возраст WS данных**: `ws_age_ms = now_ms - last_ws_tick_ms`.
- **Возраст HTTP данных**: `http_age_ms = now_ms - last_http_tick_ms`.
- **Effective age**:
  - WS → `ws_age_ms`,
  - HTTP → `http_age_ms`,
  - NONE → `9999`.

Пороговые значения:

- На входе: `data_stale = True` ⇒ вход запрещён.
- В сопровождении: выход при `effective_age_ms > 1500`.

## Фильтры входа (entry filters)

Вход в цикл запрещён, если выполнено хоть одно условие:

1. `data_stale = True` — нет актуального источника данных.
2. `spread_bps > max_spread_bps` — слишком широкий спред.
3. `tick_rate < min_tick_rate` — недостаточная активность рынка.
4. `impulse_bps < impulse_min_bps` — слабое ценовое движение.
5. **Leverage check**:
   - `total_notional = 2 * (usd_notional / mid) * mid`.
   - `equity_usdt = margin_free + spot_free`.
   - вход запрещён, если `total_notional > equity_usdt * leverage_max`.

## Расчёт количества и округление

- Базовый объём: `qty = usd_notional / mid`.
- Округление по шагу символа: `qty = floor(qty / step_size) * step_size`.
- Дополнительные проверки:
  - `qty >= min_qty`.
  - `qty * mid >= min_notional`.

## Победитель/проигравший

- Алгоритм сравнивает движение **long_raw** и **short_raw** в bps.
- Winner определяется по большему значению.
- Winner фиксируется, если `max(long_raw, short_raw) >= winner_threshold_bps`.

Если победитель не найден до `detect_timeout_ms`, цикл завершается
через аварийное выравнивание.

## Комиссии и net расчёт

- **Net bps** для победителя: `winner_raw_bps - fee_total_bps`.
- **Net bps** для проигравшего: `loser_raw_bps - fee_total_bps`.
- **Net total bps**: `winner_net + loser_net`.
- **Net USD**: `(usd_notional / 10_000) * net_total_bps`.

## Условия выхода победителя

Алгоритм завершает сопровождение победителя при любом из условий:

- `winner_net_bps >= target_net_bps` → выход по цели.
- `winner_raw_bps <= -emergency_stop_bps` → экстренный стоп.
- `winner_raw_bps <= -max_loss_bps` → обычный стоп-лосс.
- `effective_age_ms > 1500` → устаревание данных.

## Логика «aggressive limit»

Для режимов `aggressive_limit` цена устанавливается:

- BUY: `ask * (1 + slip_bps / 10_000)`.
- SELL: `bid * (1 - slip_bps / 10_000)`.

Если параметры цены некорректны, ордер отправляется как `market`.
