import os

# Superset configuration

SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'thisISaSECRET_1234')

# PostgreSQL metadata database (separate from pipeline data)
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'hackathon')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'hackathon')}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:"
    f"{os.environ.get('POSTGRES_PORT', '5432')}/"
    f"superset"
)

# Redis cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis:6379/0',
}

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Webserver
WEBSERVER_TIMEOUT = 60

# Disable CSP for local development
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
