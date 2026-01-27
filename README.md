# Directional Hedge Scalper — v0.2.1 (Cross Margin + Live Micro-Trade)

This repository contains the v0.2.1 GUI for the Directional Hedge Scalper. Live micro-trade (Cross Margin) is available when **LIVE ENABLED** is turned on.

## What’s new in v0.2.1

- Cross Margin only mode (no Spot/Futures selector).
- Spot + Cross Margin USDT balance display with periodic refresh.
- Live micro-trade execution (market/aggressive_limit) for $20 notional when LIVE ENABLED is on.
- Borrow-based leverage target selector (1x–3x).

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

- Live trading is **OFF by default**. Enable via **LIVE ENABLED** in Settings.
- The simulation uses orderbook-based fills for a single-side cycle (BUY or SELL).
