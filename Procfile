release: ./manage.py migrate
web: gunicorn --bind 0.0.0.0:$PORT multinet.wsgi
worker: REMAP_SIGTERM=SIGQUIT celery --app multinet.celery worker --loglevel INFO --without-heartbeat
