# Directional Hedge Scalper — v0.3.9 (Cross Margin + Hedge Scalping)

This repository contains the v0.3.9 GUI for the Directional Hedge Scalper. The bot runs a full hedge scalping cycle on Cross Margin when **LIVE ENABLED** is turned on.

## What’s new in v0.3.9

- Anti-freeze timeouts with safe fallback (cancel → flatten → cooldown).
- Strict single-flight cycle enforcement (no duplicate entries while a cycle is active).
- Cycle-level client order IDs with deterministic DHS-{symbol}-{cycle_id}-{leg}-{phase} tags.
- Guarded cooldown/flatten transitions with flat-position checks before re-arming.
- Trading tab status fields for inflight flags, open orders, and margin position snapshot.

## Requirements

- Python 3.10+
- Install dependencies:

```bash
pip install -r requirements.txt
```

## Run

```bash
python -m src.app
```

## 10 cycles smoke test (v0.3.9)

1. Open the GUI and set **Max cycles** to `10` (next to **СТАРТ**).
2. In **Параметры**, set **Nominal USD** to `10` and **Order mode** to `MARKET`.
3. Enable **Авто-цикл** and apply settings.
4. Click **СТАРТ** and watch the log for:
   - `[CYCLE] START n=1 ...`
   - `[CYCLE] END n=1 ...`
   - ...
   - `[CYCLE] END n=10 ...`
   - `[ENGINE] max_cycles reached -> stop`

## Stability checklist (v0.7.29.7)

- CONNECT → START and let it run for ~10 minutes.
- Observe at least 5 full ENTRY→EXIT cycles.
- On a sharp price jump, ensure:
  - No FSM freeze (watchdog triggers reconcile/recover, not a stall).
  - Either TP/SL closes the position, RECOVER places exit, or SAFE_STOP logs a clear reason.
- Logs to confirm:
  - `[ENTRY] placed` / `[ENTRY] repriced` / `[ENTRY] filled`
  - `[EXIT] maker_armed` / `[EXIT] cross_armed` / `[EXIT] filled`
  - `[RECOVER] start` / `[RECOVER] snapshot` / `[RECOVER] result`
  - `[RECONCILE_INVALID] ... order_ids=[...]`

## Запуск в один клик (Windows)

1. Скачайте репозиторий и распакуйте в удобную папку.
2. Запустите `RUN_GUI.ps1` из корня проекта.
3. Лаунчер автоматически создаст `.venv`, установит зависимости и запустит GUI.

> Примечание: при первом запуске установка зависимостей может занять несколько минут.

## Logs

- GUI live log is shown in the **Logs** tab.
- File logs are written to `logs/bot.log` (rotating).

## Notes

- Live trading is **OFF by default**. Enable via **LIVE ENABLED** in Settings.
- The bot sends real margin orders. Use at your own risk.
- **Security:** never store API keys in `config/settings.json`. Use `config/settings.local.json` (gitignored) or environment variables (`BINANCE_API_KEY`, `BINANCE_API_SECRET`).
