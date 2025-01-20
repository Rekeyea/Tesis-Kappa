import os
from celery.schedules import crontab

FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "EMBEDDED_SUPERSET": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Doris database configuration
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
SQLALCHEMY_TRACK_MODIFICATIONS = True

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_CELERY_DB = os.environ.get("REDIS_CELERY_DB", 0)
REDIS_RESULTS_DB = os.environ.get("REDIS_RESULTS_DB", 1)

# Doris SQLAlchemy connection string
ADDITIONAL_DATABASES = {
    'apache_doris': {
        'connect_args': {
            'host': os.environ.get('DORIS_HOST'),
            'port': int(os.environ.get('DORIS_PORT', 9030)),
            'user': os.environ.get('DORIS_USER'),
            'password': os.environ.get('DORIS_PASSWORD'),
            'database': os.environ.get('DORIS_DATABASE'),
        },
        'engine': 'mysqldb',
        'driver': 'pymysql',
        'encoding': 'utf8mb4'
    }
}

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': REDIS_RESULTS_DB,
}

# Celery configuration
class CeleryConfig:
    BROKER_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    CELERY_IMPORTS = (
        "superset.sql_lab",
        "superset.tasks",
    )
    CELERY_RESULT_BACKEND = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    CELERY_ANNOTATIONS = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
        "email_reports.send": {
            "rate_limit": "1/s",
            "time_limit": 120,
            "soft_time_limit": 150,
            "ignore_result": True,
        },
    }
    CELERY_TASK_PROTOCOL = 1


CELERY_CONFIG = CeleryConfig