# Directional Hedge Scalper — v0.1.0 (GUI MVP)

This repository contains the v0.1.0 GUI-only scaffold for the Directional Hedge Scalper. **No real trading or order execution is implemented in this version.**

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

- v0.1.0 provides a GUI, websocket market data feed, and state/parameter scaffolding only.
- Real order execution will be added in later versions.
