from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # LLM pour la traduction NL → plan
    LLM_API_URL: str = "https://api.scaleway.ai/.../v1"
    LLM_API_KEY: str = ""
    LLM_MODEL: str = "mistral-small-3.2-24b-instruct-2506"

    # Limites
    MAX_FILE_SIZE_MB: int = 100
    MAX_ROWS_OUTPUT: int = 100
    CACHE_TTL_SECONDS: int = 3600
    CACHE_MAX_SIZE_MB: int = 500
    QUERY_TIMEOUT_SECONDS: int = 10
    DOWNLOAD_TIMEOUT_SECONDS: int = 30

    # Service
    PORT: int = 8093
    MCP_PORT: int = 8088

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
