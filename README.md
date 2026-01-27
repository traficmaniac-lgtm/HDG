# Directional Hedge Scalper — v0.2.0 (Read-Only SIM)

This repository contains the v0.2.0 GUI for the Directional Hedge Scalper. **No real trading or order execution is implemented in this version.**

## What’s new in v0.2.0

- Read-only account checks for Spot/Margin and Futures (GET-only endpoints).
- USDT balance display with periodic refresh.
- Settings tab with local key storage (config/settings.json) and test connection.
- Read-only orderbook simulation for single-side cycles (no hedge, no orders).

## Requirements

- Python 3.10+
- Install dependencies:

```bash
pip install -r requirements.txt
```

## Run

```bash
python -m src.app.main
```

## Запуск в один клик (Windows)

1. Скачайте репозиторий и распакуйте в удобную папку.
2. Запустите `RUN_GUI.ps1` из корня проекта.
3. Лаунчер автоматически создаст `.venv`, установит зависимости и запустит GUI.

> Примечание: при первом запуске установка зависимостей может занять несколько минут.

## Logs

- GUI live log is shown in the **Logs** tab.
- File logs are written to `logs/bot.log` (rotating).

## Notes

- v0.2.0 is read-only: GET endpoints only, no order placement, no live positions.
- The simulation uses orderbook-based fills for a single-side cycle (BUY or SELL).
- Real order execution will be added in later versions.
