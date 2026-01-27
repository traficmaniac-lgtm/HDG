# Алгоритмы расчётов (метрики и фильтры)

Документ описывает фактические вычисления, которые используются в `DirectionalCycle`
и `MarketDataService`.

## Базовые вычисления цены

- **Mid price**: `mid = (bid + ask) / 2`.
- **Spread в bps**: `spread_bps = (ask - bid) / mid * 10_000`.
- **Raw bps** относительно `entry_mid`:
  - LONG: `(mid_now - entry_mid) / entry_mid * 10_000`.
  - SHORT: `(entry_mid - mid_now) / entry_mid * 10_000`.

> В live WS потоках используются Decimal (`bid_raw`, `ask_raw`, `mid_raw`) для
> избежания потерь точности при расчёте импульса.

## Выбор effective tick

Алгоритм использует `MarketDataService` для выбора актуального тика:

- `WS_ONLY` → используется только WS (если свежий).
- `HTTP_ONLY` → используется только HTTP (если свежий).
- `HYBRID` → автоматическое переключение:
  - WS «свежий», если `ws_age_ms <= 500`.
  - HTTP «свежий», если `http_age_ms <= 1200`.
  - При падении WS держится HTTP ещё `1500ms` (`http_hold_ms`).
  - Возврат на WS требует `ws_recovery_ticks` подряд свежих тиков.

Если `effective_source == NONE`, то `data_stale=True`.

## Tick rate

Tick rate измеряется по WS-такту за последние 1 секунду:

```
rx_count_1s = ws_ticks_in_last_1s
```

Если WS подключён и «свежий» (`ws_age_ms < 500`), но `rx_count_1s == 0`,
тикрейт форсируется до `1` (логируется как `forced_alive`).

## Импульс (impulse_bps)

Импульс рассчитывается **только** по последним двум WS mid-значениям (`mid_raw`).

```
impulse_bps = abs(last_ws_mid_raw - prev_ws_mid_raw) / prev_ws_mid_raw * 10_000
```

Флаг `impulse_ready` становится `True`, когда есть минимум два WS mid значения.

## Фильтры входа (entry filters)

Вход запрещён, если выполнено любое условие:

1. `data_stale=True` — отсутствует актуальный источник данных.
2. `spread_bps > max_spread_bps`.
3. `tick_rate < min_tick_rate`.
4. `impulse_bps < impulse_min_bps` (если импульс-фильтр включён).
5. Нарушение лимита по плечу (см. ниже).

### Деградация импульса

Импульс-фильтр может быть автоматически «ослаблен» после `impulse_grace_ms`:

- Если прошло достаточно времени после `ARMED`,
- И остальные фильтры (data/spread/tick rate) выполняются,
- Тогда импульс-фильтр логируется как `impulse_degraded` и не блокирует вход.

Поведение фиксируется в логах и зависит от `impulse_degrade_mode`.

## Проверка плеча (leverage_max)

Суммарная экспозиция двух ног не должна превышать equity:

```
qty = usd_notional / mid
notional_total = 2 * qty * mid
leverage_ok = notional_total <= equity_usdt * leverage_max
```

`equity_usdt` вычисляется как сумма свободных средств в маржинальном и спотовом
аккаунтах (см. `TradeEngine`).

## Расчёт количества и округление

- Базовый объём: `qty = usd_notional / mid`.
- Округление вниз по `step_size` (лот фильтр).
- Дополнительные ограничения:
  - `qty >= min_qty`.
  - `qty * mid >= min_notional`.

## Детект победителя

- Окно детекта: `detect_window_ticks` (минимум 5).
- Таймаут детекта: `detect_timeout_ms`, но не меньше
  `(detect_window_ticks / min_tick_rate) * 1000 + 500`.

Победитель фиксируется, если:

```
best = max(long_raw_bps, short_raw_bps)
if best >= winner_threshold_bps:
    winner = LONG or SHORT
```

До заполнения окна `detect_window_ticks` цикл ждёт.

## Расчёт PnL и net метрик

- **Net bps** по ноге: `raw_bps - fee_total_bps`.
- **Net total bps**: `winner_net_bps + loser_net_bps`.
- **Net USD**: `(usd_notional / 10_000) * net_total_bps`.

## Условия выхода победителя

Выход из `RIDING` по любому условию:

- `winner_net_bps >= target_net_bps` → цель.
- `winner_raw_bps <= -emergency_stop_bps` → экстренный стоп.
- `winner_raw_bps <= -max_loss_bps` → обычный стоп-лосс.
- `effective_age_ms > 1500` → данные устарели.

## Aggressive limit

Цена для `aggressive_limit`:

- BUY: `ask * (1 + slip_bps / 10_000)`.
- SELL: `bid * (1 - slip_bps / 10_000)`.

При `aggressive_limit` ожидание fill — 0.4s, затем отмена и fallback на `market`.
