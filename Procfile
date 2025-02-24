web: gunicorn app:server --timeout 30 --log-level=info
queue: celery -A app:celery_app worker --loglevel=INFO --concurrency=2 -Q magi-queue
beat: celery -A app:celery_app beat --loglevel=INFO --max-interval=30