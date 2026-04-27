from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    APP_ENV: str = "local"

    # Database
    DATABASE_URL: Optional[str] = None

    # Azure Storage Settings (replaces AWS S3)
    AZURE_STORAGE_ACCOUNT: Optional[str] = None
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = None
    AZURE_CONTAINER_NAME: Optional[str] = "datalake"        # main container (Raw/Bronze/Silver layers)
    ADLS_CONTAINER_NAME: Optional[str] = "landing"          # source landing container
    ADLS_ROOT_FOLDER: Optional[str] = None                   # virtual root prefix inside ADLS container
    AZURE_TENANT_ID: Optional[str] = None
    AZURE_CLIENT_ID: Optional[str] = None                   # Service Principal client id (optional)
    AZURE_CLIENT_SECRET: Optional[str] = None               # Service Principal secret (optional)

    # Email Settings (SMTP/Outlook)
    SMTP_SERVER: str = "smtp.office365.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAIL_FROM: Optional[str] = None
    EMAIL_TO: Optional[str] = None

    # Allow reading from .env file
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()