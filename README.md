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

### 4. Настрой .env файл
```bash
cp .env.example .env
nano .env
```

**Обязательные параметры:**
Telegram Bot
BOT_TOKEN=your_telegram_bot_token
ADMIN_ID=your_telegram_id

MEXC API
MEXC_API_KEY=your_mexc_api_key
MEXC_SECRET_KEY=your_mexc_secret_key

BSC RPC
BSC_RPC_URL=https://bsc-dataseed1.binance.org/

База данных
DATABASE_URL=postgresql://user:pass@localhost/arbitrage


### 5. Запуск
```bash
# Тестовый запуск
python bot.py

# Фоновый режим (screen)
screen -S bot
source venv/bin/activate
python bot.py
# Ctrl+A, D — свернуть

# Systemd сервис
sudo systemctl start arbitrage-bot
```

## ⚙️ Конфигурация

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `MIN_PROFIT_PCT` | Минимальная прибыль (%) | 0.5% |
| `MAX_TRADE_AMOUNT` | Макс. сумма сделки (USDT) | 100 |
| `CHECK_INTERVAL` | Интервал проверки (сек) | 2 |
| `GAS_PRICE_GWEI` | Цена газа (Gwei) | auto |

## 📊 Мониторинг
Статус: journalctl -u arbitrage-bot -f
Логи: tail -f bot.log
База: psql -d arbitrage -c "SELECT * FROM trades ORDER BY created_at DESC LIMIT 10;"


## 🛡️ Важные предупреждения ⚠️

> **❌ Я НЕ НЕСУ ОТВЕТСТВЕННОСТИ за любые финансовые потери!**
>
> - Криптовалютный арбитраж сопряжён с **высокими рисками**
> - Возможны **сбои API бирж**, **сетевые задержки**
> - **MEV-атаки**, **фронтраннинг**, **слиппедж**
> - **Комиссии** могут съесть прибыль
> - Используй **только те средства**, которые готов потерять
