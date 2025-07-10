import os
DB_HOST = "localhost"
DB_PORT = "5400"
DB_USER = "postgres"
DB_PASSWORD = "crm-issam"
DB_NAME = "postgres"
ASYNCPG_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SECRET_KEY = "dev-secret"

# URL of the local KMS service
KMS_URL = os.getenv("KMS_URL", "http://localhost:9000")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# If true, skip validation checks for onboarding data
BYPASS_ONBOARDING_CHECKS = False
BYPASS_SESSION = False


