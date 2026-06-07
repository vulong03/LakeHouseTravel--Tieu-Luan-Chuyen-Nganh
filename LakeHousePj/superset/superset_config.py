# Superset Configuration
import os

# Flask app settings
FLASK_APP = "superset.app:create_app"
FLASK_ENV = "production"
SECRET_KEY = "thisIsAVeryInsecureSecretChangeMe"

# SQLAlchemy configuration
SQLALCHEMY_DATABASE_URI = "postgresql://lakehouse_user:lakehouse_pass@postgres:5432/superset_db"
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Cache configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "simple",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_ROW_LEVEL_SECURITY": True,
    "VERSIONED_EXPORT": True,
}

# Security settings
WTF_CSRF_ENABLED = True
SESSION_COOKIE_SECURE = False  # Set to True in production with HTTPS
SUPERSET_WEBSERVER_PROTOCOL = "http"

# Time grain config
TIME_GRAIN_ADDONS = {}

# Babel config
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "vi": {"flag": "vn", "name": "Tiếng Việt"},
}
BABEL_DEFAULT_LOCALE = "en"

# Timeout settings
SUPERSET_SQLLAB_TIMEOUT = 300
SQLLAB_QUERY_COST_ESTIMATE_TIMEOUT = 10

# Export settings
EXCEL_EXTENSIONS = {"xlsx"}
EXPORT_MAX_ROWS = 10000

# Async query settings
ASYNC_QUERY_MANAGER_CLASS = "superset.extensions.AsyncQueryManager"

# Data source configuration for preview
DATASOURCE_PREVIEW_COUNT = 1000

# Allow testing database engines
TESTING_DATABASE_ENGINES = ["superset.db.engine_specs.sqlite.SqliteEngineSpec"]
