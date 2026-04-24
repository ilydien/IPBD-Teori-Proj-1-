"""Prefect Global Configuration"""

import os
from pathlib import Path

from dotenv import load_dotenv

env_file = Path(__file__).parent / ".env"
if env_file.exists():
    _ = load_dotenv(env_file)


class Settings:
    """Application settings loaded from environment variables."""

    # ============================================================
    # Database Settings
    # ============================================================
    LAKEHOUSE_HOST: str = os.getenv("POSTGRES_LAKEHOUSE_HOST", "localhost")
    LAKEHOUSE_PORT: str = os.getenv("POSTGRES_LAKEHOUSE_PORT", "5432")
    LAKEHOUSE_USER: str = os.getenv("POSTGRES_LAKEHOUSE_USER", "postgres")
    LAKEHOUSE_PASSWORD: str = os.getenv("POSTGRES_LAKEHOUSE_PASSWORD", "postgres")
    LAKEHOUSE_NAME: str = os.getenv("POSTGRES_LAKEHOUSE_DB", "postgres")

    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "5"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    DB_POOL_PRE_PING: bool = os.getenv("DB_POOL_PRE_PING", "True").lower() == "true"

    # ============================================================
    # Prefect Settings
    # ============================================================
    PREFECT_BLOCK_NAME: str = os.getenv("PREFECT_BLOCK_NAME", "postgres-demo-block")
    DEFAULT_TABLE_NAME: str = os.getenv("DEFAULT_TABLE_NAME", "posts")
    DEFAULT_LIMIT: int = int(os.getenv("DEFAULT_LIMIT", "20"))

    # ============================================================
    # API Fetch Settings
    # ============================================================
    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "30"))
    API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))
    API_RETRY_DELAYS: list[int] = [2, 5, 10]

    # ============================================================
    # Derived Properties
    # ============================================================
    @classmethod
    def get_db_connection_string(cls, async_driver: bool = False) -> str:
        """Build database connection string from settings."""
        driver = "postgresql+asyncpg" if async_driver else "postgresql"
        return (
            f"{driver}://{cls.LAKEHOUSE_USER}:{cls.LAKEHOUSE_PASSWORD}"
            f"@{cls.LAKEHOUSE_HOST}:{cls.LAKEHOUSE_PORT}/{cls.LAKEHOUSE_NAME}"
        )

    @classmethod
    def validate_db_settings(cls) -> bool:
        """Validate all required database settings are present."""
        required = [
            "POSTGRES_LAKEHOUSE_USER",
            "POSTGRES_LAKEHOUSE_PASSWORD",
            "POSTGRES_LAKEHOUSE_HOST",
            "POSTGRES_LAKEHOUSE_PORT",
            "POSTGRES_LAKEHOUSE_NAME",
        ]
        missing = [var for var in required if not getattr(cls, var)]

        if missing:
            print(f"⚠️ Missing environment variables: {missing}")
            return False
        return True

    @classmethod
    def to_dict(cls) -> dict:
        """Convert settings to dictionary for debugging."""
        # Exclude sensitive data
        return {
            "database": {
                "host": cls.LAKEHOUSE_HOST,
                "port": cls.LAKEHOUSE_PORT,
                "name": cls.LAKEHOUSE_NAME,
                "user": cls.LAKEHOUSE_USER,
                "pool_size": cls.DB_POOL_SIZE,
            },
            "prefect": {
                "block_name": cls.PREFECT_BLOCK_NAME,
                "default_table": cls.DEFAULT_TABLE_NAME,
            },
        }


settings = Settings()
