import os
import redis

BASEDIR = os.path.abspath(os.path.dirname(__file__))
SQLITE_DB = 'sqlite:///' + os.path.join(BASEDIR, 'db.sqlite')


class Config(object):
    DEBUG = False
    SECRET_KEY = 'its-too-simple'

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:123@localhost:3306/db"

    CELERY_TIMEZONE = 'UTC'
    BROKER_URL = 'redis://localhost:6379/0'
    CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
    redbeat_redis_url = "redis://localhost:6379/0"
    CELERY_SEND_TASK_SENT_EVENT = True


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:123@localhost:3306/db'


class ProductionConfig(Config):
    pass

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}
