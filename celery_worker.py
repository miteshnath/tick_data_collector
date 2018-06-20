from celery import Celery

from bittrex import create_app


def create_celery(app):
    celery = Celery(app.import_name,
                backend=app.config['CELERY_RESULT_BACKEND'],
                broker=app.config['BROKER_URL'])
    celery.conf.update(app.config)
    #celery.autodiscover_tasks()
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

flask_app = create_app()
celery = create_celery(flask_app)

flask_app.app_context().push()