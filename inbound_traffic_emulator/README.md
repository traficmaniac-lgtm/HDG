# Inbound Traffic Emulator (Windows)

Утилита для эмуляции входящего TCP-трафика на уровне WinDivert.

## Возможности

- Режимы: `FREEZE`, `LAG`, `THROTTLE`, `LOSS`
- Фильтрация по:
  - Remote IP
  - Remote Port
  - Process (PID lookup через `psutil`)
  - Interface Index
- ACK-пакеты без payload пропускаются без задержек
- Поддержка `Hard Freeze` (буферизация до STOP)
- На STOP буфер мгновенно сбрасывается обратно в стек

## Ограничения

- Только Windows 10/11
- Нужны права администратора
- Нужен установленный WinDivert-драйвер (`WinDivert.dll` + `.sys`)

## Установка

```bash
pip install -r inbound_traffic_emulator/requirements.txt
```

## Запуск

```bash
python inbound_traffic_emulator/main.py
```

## Примечания по фильтру

Базовый фильтр строится в формате WinDivert, например:

```text
tcp and inbound and ip.SrcAddr == 109.105.133.38 and tcp.SrcPort == 7777
```

Дополнительно фильтрация по PID делается в Python-слое (по локальному порту назначения).
