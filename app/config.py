"""Settings read from the environment. One image, role chosen by ROLE."""
import os


def _env(name, default=None):
    return os.environ.get(name, default)


ROLE = _env("ROLE", "api")
INSTANCE = _env("INSTANCE", ROLE)

PG_DSN = (
    f"host={_env('PG_HOST', 'localhost')} port={_env('PG_PORT', '5432')} "
    f"dbname={_env('POSTGRES_DB', 'medigrid')} user={_env('POSTGRES_USER', 'medigrid')} "
    f"password={_env('POSTGRES_PASSWORD', '')} application_name={INSTANCE}"
)
# Pool sizes (logged in DECISIONS.md D-POOL). 2 APIs x 24 + 2 workers x 14 + tools 24 < 200.
PG_POOL_MAX = int(_env("PG_POOL_MAX", "24" if ROLE in ("api", "tools") else "14"))

REDIS_HOST = _env("REDIS_HOST", "localhost")
REDIS_PORT = int(_env("REDIS_PORT", "6379"))
REDIS_TIMEOUT_S = float(_env("REDIS_TIMEOUT_MS", "150")) / 1000.0

MQ_HOST = _env("MQ_HOST", "localhost")
MQ_USER = _env("RABBITMQ_DEFAULT_USER", "guest")
MQ_PASS = _env("RABBITMQ_DEFAULT_PASS", "guest")
MQ_MGMT_URL = _env("MQ_MGMT_URL", "http://localhost:15672")

SECRET_KEY = _env("SECRET_KEY", "dev-only")
CHAOS_ENABLED = _env("CHAOS_ENABLED", "0") == "1"
API_INTERNAL_URLS = [u for u in _env("API_INTERNAL_URLS", "http://localhost:8001,http://localhost:8002").split(",") if u]
NGINX_URL = _env("NGINX_URL", "http://localhost:8080")

# Domain settings
DEFAULT_TTL_S = 900
SEARCH_RADIUS_KM = 50.0
MAX_RADIUS_KM = 150.0
RATE_MAX = int(_env("RATE_MAX", "20"))
RATE_WINDOW_MS = int(_env("RATE_WINDOW_MS", "10000"))
LUA_ATTEMPTS = 3
BED_TYPES = ("ICU", "VENTILATOR", "GENERAL")
PRIORITIES = {1: "routine", 2: "standard", 3: "urgent", 4: "critical"}
