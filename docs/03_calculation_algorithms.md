# Алгоритмы расчётов (метрики и фильтры)

Этот документ описывает формулы и вычисления, используемые алгоритмом.

## Базовые вычисления цены

- **Mid price**: `mid = (bid + ask) / 2` (либо приходит напрямую в тике).
- **Spread в bps**: `spread_bps = (ask - bid) / mid * 10_000`.
- **Raw bps**:
  - LONG: `(to_mid - from_mid) / from_mid * 10_000`.
  - SHORT: `(from_mid - to_mid) / from_mid * 10_000`.

## Импульс и скорость тиков

- **Tick rate** — количество тиков за последнюю секунду: `ticks_last_1s / 1s`.
- **Импульс (impulse_bps)** — абсолютное изменение mid между двумя последними тиками:
  `abs(mid - prev_mid) / prev_mid * 10_000`.

## Проверка свежести данных

- **Возраст данных**: `now_utc - tick.rx_time` (в мс).
- Данные считаются устаревшими, если:
  - возраст > 500 мс, **или**
  - источник не `WS` (fallback/HTTP).

## Фильтры входа (entry filters)

Вход в цикл запрещён, если выполнено хоть одно условие:

1. `spread_bps > max_spread_bps` — слишком широкий спред.
2. `tick_rate < min_tick_rate` — недостаточная активность рынка.
3. `impulse_bps < impulse_min_bps` — слабое ценовое движение.
4. `data_stale = True` — устаревшие или не-WS данные.

## Расчёт количества

- Базовый объём: `qty = usd_notional / mid`.
- Округление по шагу символа: `qty = floor(qty / step_size) * step_size`.
- Дополнительные проверки:
  - `qty >= min_qty`.
  - `qty * mid >= min_notional`.

## Победитель/проигравший

- Алгоритм сравнивает движение **long_raw** и **short_raw** в bps.
- Winner определяется по большему значению.
- Winner фиксируется, если `max(long_raw, short_raw) >= winner_threshold_bps`.

## Расчёт чистого результата

- **Net bps** для победителя: `winner_raw_bps - fee_total_bps`.
- **Net bps** для проигравшего: `loser_raw_bps - fee_total_bps`.
- **Net total bps**: `winner_net + loser_net`.
- **Net USD**: `(usd_notional / 10_000) * net_total_bps`.
