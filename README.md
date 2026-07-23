# Bitrix24 Deal Picker — Railway

Перенос приложения «ПОЛУЧИТЬ ЗАЯВКУ» с VibeCode на Railway.

- **Стек:** Python 3, stdlib only (без внешних зависимостей).
- **Версия приложения:** `2026-07-23-batched-deal-search`
- **Хранилище состояния:** JSON-файлы в Railway Volume (`APP_DATA_DIR=/data`).
- **Сборка:** Dockerfile (`python:3.12-slim`).
- **Healthcheck:** `/api/health` (Railway проверяет автоматически).

---

## Структура репозитория

```
.
├── app.py               # приложение (без изменений из исходного ZIP)
├── managers.json        # стартовый список менеджеров (seed)
├── entrypoint.sh        # init APP_DATA_DIR перед запуском
├── Dockerfile           # образ python:3.12-slim
├── railway.json         # конфигурация деплоя Railway
├── Procfile             # альтернативный запуск (если без Docker)
├── requirements.txt     # пустой (зависимостей нет)
├── .env.example         # все переменные с пояснениями
├── .dockerignore
├── .railwayignore
└── .gitignore
```

---

## 1. Деплой на Railway

### 1.1 Создание сервиса

1. Зайдите на [railway.app](https://railway.app) → **New Project** →
   **Deploy from GitHub repo** (или загрузите этот репозиторий).
2. Railway определит `Dockerfile` и соберёт образ.
3. Сервис получит адрес `https://<service>-<random>.up.railway.app`.
   Зафиксируйте его — это `PUBLIC_APP_URL`.

### 1.2 Подключение Volume (обязательно)

1. В сервисе → **Settings** → **Volumes** → **Add Volume**.
2. Mount path: `/data`.
3. Это постоянное хранилище для:
   - `managers.json` (статический список компетенций)
   - `access_rules.json` (лимиты, админ-настройки)
   - `claim_log.json` (история принятых заявок)
   - `reject_log.json` (история отказов)
   - `greeting_log.json` (история приветствий)

> ⚠️ **Без Volume данные теряются при каждом редеплое.**

### 1.3 Переменные окружения

В сервисе → **Variables** задайте (см. `.env.example` для пояснений):

| Переменная | Значение |
|---|---|
| `BITRIX_WEBHOOK_BASE` | `https://krugosvet.bitrix24.kz/rest/41/XXXXXXXXXX/` (секрет — не коммитить) |
| `DRY_RUN` | `0` |
| `HOST` | `0.0.0.0` |
| `PORT` | `3000` |
| `PUBLIC_APP_URL` | `https://<ваш-домен>.up.railway.app` |
| `ADMIN_USER_IDS` | `41` |
| `ALLOW_UNVERIFIED_USERS` | `0` |
| `APP_TZ_OFFSET_HOURS` | `6` |
| `GREETING_AUTO_SEND` | `1` |
| `BITRIX_TIMEOUT_SECONDS` | `12` |
| `BITRIX_FAST_TIMEOUT_SECONDS` | `5` |
| `LIMIT_FREE_WINDOW_START` | `18:00` |
| `LIMIT_FREE_WINDOW_END` | `21:30` |
| `NEXT_DEAL_SCAN_LIMIT` | `8` |
| `NEXT_DEAL_SCAN_WORKERS` | `8` |
| `NEXT_DEAL_BATCH_TIMEOUT_SECONDS` | `12` |
| `DEAL_ANALYSIS_CACHE_TTL_SECONDS` | `300` |
| `APP_DATA_DIR` | `/data` |

> 🔒 `BITRIX_WEBHOOK_BASE` — секрет. Задавайте через Railway Variables
> (Raw mode), **не** через файл в репозитории.

---

## 2. Перенос данных с VibeCode (КРИТИЧНО)

> Выполнять **до** отключения VibeCode. Иначе данные потеряются безвозвратно.

### Что перенести

Из работающего VibeCode-приложения выгрузить 5 файлов:

1. `managers.json` — менеджеры и компетенции
2. `access_rules.json` — лимиты, админ-настройки
3. `claim_log.json` — статистика принятых заявок
4. `reject_log.json` — история отказов
5. `greeting_log.json` — история отправленных приветствий

### Подготовка: выгрузка с VibeCode

На стороне работающего VibeCode-инстанса упаковать 5 файлов состояния:

```bash
# Выполнять на VibeCode (SSH/консоль платформы).
cd <папка приложения VibeCode>
tar czf /tmp/bitrix-state.tar.gz \
  managers.json access_rules.json claim_log.json \
  reject_log.json greeting_log.json
# Скопировать /tmp/bitrix-state.tar.gz на локальную машину
# (scp, скачать через панель VibeCode — что доступно).
```

### Загрузка в Volume Railway

Установить [Railway CLI](https://docs.railway.app/develop/cli) и привязать проект:

```bash
npm i -g @railway/cli
railway login
railway link   # выбрать проект/окружение/сервис с подключённым Volume
```

`railway ssh` открывает интерактивную сессию **в удалённом контейнере**
с примонтированным Volume. Копируем архив внутрь и распаковываем в `/data`.

#### Шаг 1. Копирование архива в контейнер

Из локального терминала (архив уже скачан с VibeCode):

```bash
# railway ssh умеет прокидывать stdin в удалённый контейнер.
# Копируем tar-архив в Volume, затем распаковываем.
cat /tmp/bitrix-state.tar.gz | railway ssh "cat > /tmp/state.tar.gz"
```

#### Шаг 2. Распаковка в Volume

```bash
railway ssh
# --- теперь мы в удалённом контейнере, Volume примонтирован в /data ---
cd /data
tar xzf /tmp/state.tar.gz
ls -la /data
# Должно быть 5 файлов:
#   access_rules.json  claim_log.json  greeting_log.json
#   managers.json      reject_log.json
cat /data/access_rules.json | head   # не пустой
cat /data/claim_log.json     | head   # не пустой
exit
```

#### Альтернатива: ручная загрузка через heredoc (если файлы небольшие)

```bash
railway ssh
# --- в контейнере ---
cd /data
# Вставить содержимое каждого файла через heredoc, например:
cat > access_rules.json <<'EOF'
{ ... содержимое ... }
EOF
exit
```

> ⚠️ **Файлы нельзя перезаписывать пустыми.** Приложение при первом
> запуске создаёт недостающие файлы со значениями по умолчанию.
> Если Volume уже инициализирован пустыми файлами — перед загрузкой
> удалите их (`rm /data/*.json` в `railway ssh`), затем распакуйте
> настоящие данные с VibeCode. `managers.json` из seed-образа
> будет перезаписан настоящим из архива — это ожидаемо и правильно.

---

## 3. Smoke-тест после деплоя

### 3.1 Health

```bash
curl -s https://<домен>.up.railway.app/api/health | python3 -m json.tool
```

Ожидаемый ответ (ключевые поля):
```json
{
  "ok": true,
  "version": "2026-07-23-batched-deal-search",
  "sourceStages": {
    "UC_ZJ55BR": "Необработанные ЛИДЫ",
    "UC_PUCAAQ": "ОЖИДАЮТ СПЕЦИАЛИСТА"
  },
  "dryRun": false,
  "greetingAutoSend": true
}
```

### 3.2 Next-deal (под администратором id 41)

```bash
curl -s "https://<домен>.up.railway.app/api/next-deal?managerId=41" | python3 -m json.tool
```

Ожидается: JSON (не HTML, не таймаут). Проверить:
- заявка со стадии `UC_ZJ55BR` или `UC_PUCAAQ`;
- старая подходящая заявка выдаётся раньше новой;
- если страна/сообщения не определены — заявку может получить
  любой менеджер.

### 3.3 Полная проверка через Bitrix24

1. В локальном приложении Bitrix24 открыть приложение «ПОЛУЧИТЬ
   ЗАЯВКУ» под **администратором** (id 41) — проверить админ-панель,
   лимиты, статистику.
2. Открыть под **обычным менеджером** — проверить приём заявки:
   - после принятия менеджер становится ответственным;
   - сделка переходит в стадию `NEW`;
   - приветственное сообщение отправляется автоматически.
3. Только после полной проверки — поменять в локальном приложении
   Bitrix24:
   - **путь обработчика** → новый HTTPS-адрес;
   - **путь первоначальной установки** → новый HTTPS-адрес.
4. Отключить VibeCode.

---

## 4. Риски и ограничения

- **Один писатель.** Из-за файлового состояния держите 1 реплику.
  Горизонтальное масштабирование (>1 реплики) сломает
  консистентность JSON-файлов.
- **Volume обязателен.** Без него данные теряются при редеплое.
- **Стабильный URL.** Используйте закреплённый `*.up.railway.app`
  или привязанный кастомный домен. Не пересоздавайте сервис —
  при пересоздании URL меняется, придётся перенастраивать Bitrix24.
- **Таймауты `/api/next-deal`** могут упираться во внешние вызовы
  Bitrix24, а не в Railway — поведение идентично VibeCode.
- **Токен вебхука.** После переноса ротируйте токен `BITRIX_WEBHOOK_BASE`
  на стороне Bitrix24, если есть подозрение утечки.

---

## 5. Локальный запуск (для отладки)

```bash
export HOST=0.0.0.0
export PORT=3000
export APP_DATA_DIR=/tmp/bitrix-data
mkdir -p "$APP_DATA_DIR"
cp managers.json "$APP_DATA_DIR/"
export BITRIX_WEBHOOK_BASE=https://krugosvet.bitrix24.kz/rest/41/XXXXXXXX/
export DRY_RUN=1
export PUBLIC_APP_URL=http://localhost:3000
export ADMIN_USER_IDS=41
export APP_TZ_OFFSET_HOURS=6
export GREETING_AUTO_SEND=0
python3 app.py
```

Открыть: http://localhost:3000/api/health
