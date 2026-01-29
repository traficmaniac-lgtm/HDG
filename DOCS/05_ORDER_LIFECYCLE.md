# Order Lifecycle

## Основной цикл
1. **START (GUI)** → `TradeExecutor.start_cycle_run()`.
2. **PLACE_BUY**: расчёт цены/количества и размещение лимитного BUY.
3. **WAIT_BUY**: ожидание исполнения и логика «follow‑top».
4. **POSITION_OPEN**: позиция открыта, мониторинг TP/SL.
5. **PLACE_SELL**: выставление SELL (LIMIT/MARKET по настройкам).
6. **WAIT_SELL**: ожидание исполнения, контроль TTL и повторов.
7. **CLOSED → IDLE**: завершение цикла и подготовка следующего.

## Хранение ордеров
- Активные ордера хранятся в `active_test_orders`.
- Для каждого ордера сохраняются `orderId`, `side`, `price`, `qty`, `status`, `cum_qty`, `avg_fill_price` и таймстемпы.

## Обновление статусов
### Через REST‑polling (OrderTracker)
- По таймеру запрашиваются статусы каждого активного ордера.
- Статусы:
  - `PARTIALLY_FILLED` → событие partial с `cum_qty` и `avg_fill_price`.
  - `FILLED` → событие filled, ордер удаляется из active‑списка.
  - `CANCELED`/`REJECTED`/`EXPIRED` → событие done, ордер удаляется.

### Через `sync_open_orders()`
- Сопоставление локального списка с `openOrders`.
- Обновление `status` и `sell_status` для SELL.

## Частичные исполнения
- BUY partial фиксирует `cum_qty` и может привести к созданию **partial‑позиции** при таймауте входа.
- SELL partial снижает остаток позиции (`_aggregate_sold_qty`) и пересчитывает оставшийся объём.

## Вход (BUY) и follow‑top
- BUY выставляется по `bid + entry_offset_ticks * tick_size`.
- Репрайс возможен если:
  - bid продвинулся минимум на `entry_reprice_min_ticks`.
  - прошёл `entry_reprice_cooldown_ms`.
  - данные стабильны (`entry_reprice_require_stable_source` и grace‑период).
  - выполнено требование `entry_reprice_min_consecutive_fresh_reads`.
- Старый BUY отменяется, создаётся новый с пересчитанной ценой/количеством.

## Таймауты входа
- **PRICE_WAIT_TIMEOUT**: нет валидной цены более `max_wait_price_ms`.
- **ENTRY_TIMEOUT**: общий лимит ожидания `max_entry_total_ms`.
- При истечении лимита возможна фиксация частичного BUY и отмена остатка.

## Выход (SELL), TTL и ретраи
- SELL выставляется, когда есть `exit_intent` (TP/SL/Manual).
- TTL `sell_ttl_ms`: при превышении — попытка отмены и повторного SELL.
- Количество повторов ограничено `max_sell_retries`.
- При критическом истечении TTL возможен принудительный MARKET‑SELL (если разрешён `force_close_on_ttl`).
- Watchdog `max_wait_sell_ms` следит за зависшими SELL и, при `allow_force_close`, может инициировать emergency‑market закрытие.

## single‑flight
- Одновременно допускается только один активный торговый поток.
- Все действия блокируются флагами `sell_place_inflight`, `sell_cancel_pending`, `inflight_trade_action`.
