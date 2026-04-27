from pydantic import BaseModel
from typing import List, Optional

class SourceConfig(BaseModel):
    type: str
    host: Optional[str]
    port: Optional[int]
    endpoint: Optional[str]
    username: Optional[str]
    remote_path: Optional[str]

class IngestionConfig(BaseModel):
    file_format: str
    delimiter: Optional[str] = ","
    header: bool = True

class ColumnConfig(BaseModel):
    name: str
    type: str
    nullable: bool = True

class SchemaConfig(BaseModel):
    table_name: str
    columns: List[ColumnConfig]

class DestinationConfig(BaseModel):
    bronze_path: str
    silver_path: str

class PipelineConfig(BaseModel):
    source: SourceConfig
    ingestion: IngestionConfig
    dataset_schema: SchemaConfig
    destination: DestinationConfig
