FROM python:3.12-slim

# Railway прокидывает PORT автоматически; приложение слушает HOST:PORT.
ENV HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Зависимостей нет — приложение на stdlib.
# Копируем только код; managers.json уйдёт в /data (см. ниже).
COPY app.py ./
COPY managers.json /app/seed-managers.json

# Точка монтирования Railway Volume для JSON-файлов состояния.
# APP_DATA_DIR должен указывать сюда, чтобы данные переживали редеплой.
RUN mkdir -p /data

# entrypoint: если Volume пуст (первый запуск), сеем managers.json.
# Существующие runtime-файлы (access_rules, claim_log, reject_log,
# greeting_log) никогда не перезаписываем — они migrate извне.
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 3000

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "app.py"]
