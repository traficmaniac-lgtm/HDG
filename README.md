# Directional Hedge Scalper — v0.3.0 (Cross Margin + Hedge Scalping)

This repository contains the v0.3.0 GUI for the Directional Hedge Scalper. The bot runs a full hedge scalping cycle on Cross Margin when **LIVE ENABLED** is turned on.

## What’s new in v0.3.0

- Full directional hedge scalping cycle: BUY + SELL market entry, cut the loser fast, hold the winner to target.
- New impulse filters (spread, tick rate, impulse move) to avoid flat entries.
- Auto-loop cycle support with cooldown timers.
- Trade engine moved to a worker thread to keep the GUI responsive.
- GUI localized to Russian.

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
- The bot sends real margin orders. Use at your own risk.
