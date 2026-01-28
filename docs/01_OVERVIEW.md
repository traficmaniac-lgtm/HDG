# Overview

Проект: Directional Hedge Scalper — GUI-клиент для Binance Cross Margin.

Назначение:
- ручной запуск тестового цикла BUY → SELL в Cross Margin через GUI;
- мониторинг маркет-данных, ордеров и простого PnL.

Текущая версия: **v0.5.9**.

Что уже работает (факты):
- GUI на PySide6, запуск через `python -m src.app`.
- Получение котировок по WS (bookTicker) с HTTP fallback.
- Роутер котировок с учётом свежести данных и источника.
- REST доступ к Binance: server time, exchangeInfo, margin account, open orders, размещение/отмена ордеров.
- Размещение BUY ордера (LIMIT или MARKET), затем ручное закрытие SELL.
- Трекинг открытых ордеров, статусов, позиции и простого PnL в интерфейсе.
- Настройки сохраняются в `config/settings.json` и доступны из GUI.
