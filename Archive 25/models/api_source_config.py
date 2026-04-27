from sqlalchemy import Column, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from core.database import Base
import uuid


class APISourceConfig(Base):
    """
    Stores dynamic API source configuration per client.
    One client can have multiple API configs (different source_name).

    Register via:  POST /api-source/register
    Use via:       POST /orchestrate/run?source_type=API&client_name=X&folder_path=<endpoint>
    """
    __tablename__ = "api_source_config"

    id             = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_name    = Column(String, nullable=False, index=True)
    source_name     = Column(String, nullable=False)
    source_type     = Column(String, default="API")
    base_url        = Column(String, nullable=True)

    auth_type       = Column(String, default="none")
    auth_token      = Column(Text,   nullable=True)
    api_key_header  = Column(String, default="X-Api-Key")
    endpoints       = Column(Text,   nullable=True)

    # AWS S3 Fields
    aws_bucket_name = Column(String, nullable=True)
    aws_region      = Column(String, nullable=True)
    aws_access_key  = Column(String, nullable=True)
    aws_secret_key  = Column(Text,   nullable=True)

    # Azure ADLS Fields
    azure_account_name   = Column(String, nullable=True)
    azure_container_name = Column(String, nullable=True)
    azure_account_key    = Column(Text,   nullable=True)

    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())

    def to_dict(self):
        return {
            "id":             self.id,
            "client_name":    self.client_name,
            "source_name":    self.source_name,
            "source_type":    self.source_type,
            "base_url":       self.base_url,
            "auth_type":      self.auth_type,
            "api_key_header": self.api_key_header,
            "endpoints":      [e.strip() for e in self.endpoints.split(",") if e.strip()] if self.endpoints else [],
            "aws_bucket":     self.aws_bucket_name,
            "bucket_name":    self.aws_bucket_name,
            "azure_account":  self.azure_account_name,
            "azure_container": self.azure_container_name,
            "is_active":      self.is_active,
            "created_at":     str(self.created_at),
        }