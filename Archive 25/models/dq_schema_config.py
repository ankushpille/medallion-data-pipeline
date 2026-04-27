from sqlalchemy import Column, String, Boolean, DateTime, Enum, ForeignKey, Index
from core.database import Base
from datetime import datetime
import enum
import uuid

class ExpectedDataType(str, enum.Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"

class DQRule(str, enum.Enum):
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    RANGE = "RANGE"
    REGEX = "REGEX"
    REFERENTIAL = "REFERENTIAL"
    CUSTOM = "CUSTOM"

class Severity(str, enum.Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    BLOCK = "BLOCK"

class DQSchemaConfig(Base):
    __tablename__ = "dq_schema_config"

    id = Column(String(255), primary_key=True, default=lambda: str(uuid.uuid4()))
    dataset_id = Column(String(255), nullable=False)
    column_name = Column(String(255), nullable=False)
    expected_data_type = Column(Enum(ExpectedDataType), nullable=True)
    dq_rule = Column(Enum(DQRule), nullable=True)
    rule_value = Column(String(255), nullable=True)
    severity = Column(Enum(Severity), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (Index("idx_dq_dataset_column", dataset_id, column_name),)