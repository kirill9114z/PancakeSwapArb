# 🚀 PancakeSwap ↔ MEXC Арбитражный Бот

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Автоматический арбитражный бот между PancakeSwap (BSC) и MEXC**. Отслеживает разницу цен, исполняет сделки и отправляет уведомления в Telegram.

## ✨ Возможности

- 🔄 **Арбитраж в реальном времени** между PancakeSwap ↔ MEXC
- 🌐 **Поддержка BSC и Spot рынков**
- 💰 **Автоматическое исполнение сделок** при профитной разнице
- 🦾 **Возможность работы до 20 торговых пар
- 📱 **Telegram уведомления** о сделках и возможностях
- 🗄️ **База данных** sqlite для истории сделок
- ⚡ **Асинхронная архитектура** (asyncio, aiohttp)
- 🛡️ **Защита от MEV и фронтраннинга**

## 🛠️ Технологии

| Компонент | Технология |
|-----------|------------|
| Язык | Python 3.10+ |
| DEX | Web3.py, PancakeSwap Router V2 |
| CEX | CCXT (MEXC) |
| База данных | PostgreSQL + SQLAlchemy |
| Telegram | Aiogram 3.x |
| Сеть | BSC Mainnet |
| Мониторинг | asyncio, websockets |

## 📋 Быстрый старт

### 1. Клонируй репозиторий
```bash
git clone <your-repo>
cd ArbitragePank
```

### 2. Создай виртуальное окружение
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
```

### 3. Установи зависимости
```bash
pip install -r requirements.txt
```

### 4. Обязательные параметры config:
```bash
BOT_TOKEN=your_telegram_bot_token
```

### 5. Запуск
```bash
# Тестовый запуск
python run.py

# Фоновый режим (screen)
screen -S bot
source venv/bin/activate
python run.py
# Ctrl+A, D — свернуть

# Systemd сервис
sudo systemctl start arbitrage-bot
```

## 📁 Структура проекта

```
run.py                  Точка входа: поднимает Telegram-интерфейс
config.py               Все настройки и пороги (создаётся из config.py.example)
abi/                    Общие ABI: роутер PancakeSwap, ERC20, Multicall3
pair_abi/               ABI пулов конкретных пар (сохраняет бот)

arb/
├── paths.py            Пути к файлам проекта
├── storage/            SQLite: пары, ключи, спреды
├── exchanges/          MEXC: приватное веб-API для выставления ордеров
├── dex/                PancakeSwap
│   ├── pancake.py      OkxTrade: сборка объекта пула + WS-цикл цены
│   ├── pool_state.py   Цена и резервы пула, обработка событий Swap
│   ├── quoting.py      Котировка под объём: локальная и on-chain
│   ├── swap_fast.py    Быстрый своп через известный пул (горячий путь)
│   ├── swap_universal.py  Универсальный своп с поиском маршрута (фоллбэк)
│   ├── allowances.py   Approve роутеру
│   └── types.py        SwapResult / SwapErrorType
├── core/               Торговая логика
│   ├── arbitrage.py    Arbitrage: анализ возможностей, остановка пары
│   ├── balances.py     Балансы на MEXC и в кошельке BSC
│   ├── market_data.py  Стакан MEXC, оценка комиссий
│   ├── orderbook.py    Чистая математика по стакану (без сети)
│   ├── execution.py    Исполнение сделки: ордер + хедж
│   ├── emergency.py    Аварийное закрытие позиции, если хедж не прошёл
│   └── runner.py       Запуск мониторинга по каждой паре
├── telegram/           Интерфейс управления (aiogram)
└── tools/              Диагностика (не участвует в работе бота)
```

Диагностика:
```bash
python -m arb.tools.debug_quote_local [ПАРА]   # локальная цена против on-chain
python -m arb.tools.check_pair_isolation       # не делят ли пары кошелёк/аккаунт
```

## ⚙️ Конфигурация

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `MIN_PROFIT_PCT` | Минимальная прибыль (%) | 0.5% |
| `MAX_TRADE_AMOUNT` | Макс. сумма сделки (USDT) | 100 |
| `CHECK_INTERVAL` | Интервал проверки (сек) | 2 |
| `GAS_PRICE_GWEI` | Цена газа (Gwei) | auto |



## 🛡️ Важные предупреждения ⚠️

> **❌ Я НЕ НЕСУ ОТВЕТСТВЕННОСТИ за любые финансовые потери!**
>
> - Криптовалютный арбитраж сопряжён с **высокими рисками**
> - Возможны **сбои API бирж**, **сетевые задержки**
> - **MEV-атаки**, **фронтраннинг**, **слиппедж**
> - **Комиссии** могут съесть прибыль
> - Используй **только те средства**, которые готов потерять
