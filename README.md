
## run celery worker
```
celery -A celery_worker:celery worker --loglevel=DEBUG
```

## run celery beat for periodic tasks
```
celery beat -A celery_worker.celery -S redbeat.RedBeatScheduler  --loglevel=info
```
