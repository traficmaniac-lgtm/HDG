# Ответственность каждого файла

Ниже перечислены ключевые файлы и их ответственность.

## Корень репозитория

- `.env.example` — шаблон переменных окружения для API ключей/режимов.
- `.gitignore` — исключения для Git.
- `.gitkeep` — удерживает пустую директорию в репозитории.
- `README.md` — описание продукта и базовые инструкции.
- `requirements.txt` — список Python-зависимостей.
- `RUN_GUI.ps1` — скрипт автозапуска GUI на Windows.

## `docs/`

- `01_project_structure.md` — структура репозитория и связи подсистем.
- `02_file_responsibilities.md` — назначение файлов (этот документ).
- `03_calculation_algorithms.md` — формулы и вычисления метрик/фильтров.
- `04_trading_algorithm_cycle.md` — описание торгового цикла и состояний.
- `05_technical_characteristics.md` — технические детали, режимы и параметры.
- `06_configuration_and_operations.md` — конфигурация, UI, рабочие сценарии.

## `src/`

### `src/app/`

- `__init__.py` — пакет приложения.
- `main.py` — точка входа GUI: запуск `TradeEngine`, потоков данных и UI.

### `src/core/`

- `__init__.py` — пакет ядра.
- `config_store.py` — сохранение/загрузка настроек (`config/`, `.env.local`).
- `logger.py` — настройка логирования и ротации (`logs/bot.log`).
- `models.py` — dataclass-модели: параметры стратегии, снимки рынка, телеметрия.
- `state_machine.py` — конечный автомат состояний торгового цикла.
- `trade_engine.py` — оркестратор цикла, автоцикла, подключения и сигналов.
- `version.py` — версия/метаданные приложения.

### `src/engine/`

- `__init__.py` — пакет торговых алгоритмов.
- `directional_cycle.py` — основной цикл hedge-скальпинга: вход, детект,
  отсечение проигравшей ноги, сопровождение победителя, выход, аварийные сценарии.
- `test_mode.py` — тестовый режим/эмуляции.

### `src/exchange/`

- `__init__.py` — пакет биржевого слоя.
- `binance_margin.py` — исполнение ордеров на Binance Cross Margin: выставление,
  ожидание fill, отмена, repay и проверка статуса.

### `src/services/`

- `__init__.py` — пакет сервисов.
- `binance_rest.py` — REST клиент Binance: подпись запросов, обработка ошибок,
  exchange info, аккаунт, ордера, depth.
- `time_sync.py` — синхронизация времени с Binance (offset, timestamp).
- `ws_market.py` — WebSocket поток для `bookTicker` и `depth10@100ms`.
- `http_fallback.py` — HTTP fallback тикер `bookTicker`.
- `market_data.py` — агрегатор тиков и выбор «effective tick».
- `orderbook.py` — хранение/обновление стакана и расчёт VWAP.

### `src/gui/`

- `__init__.py` — пакет UI.
- `main_window.py` — главное окно GUI и связка вкладок.
- `parameters_tab.py` — вкладка параметров стратегии.
- `settings_tab.py` — вкладка настроек/подключения.
- `widgets.py` — общие виджеты и UI-утилиты.

## `tests/`

- `test_tickrate_ws_alive.py` — проверки тикрейта и «живого» WS.
- `test_impulse_tickrate.py` — тесты импульса и тикрейта.
- `test_detect_window.py` — проверки окна детекта и тайминга.
- `test_margin_sideeffect.py` — sideEffectType для маржинальных ордеров.
