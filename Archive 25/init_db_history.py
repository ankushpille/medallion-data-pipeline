import sys
import os
from dotenv import load_dotenv

# Load .env variables before anything else
load_dotenv()

# Add the current directory to sys.path
sys.path.append(os.getcwd())

from core.database import engine, Base
# Import ALL models including the metadata ones
from models.master_config import MasterConfig
from models.master_config_authoritative import MasterConfigAuthoritative
from models.dq_schema_config import DQSchemaConfig
from models.api_source_config import APISourceConfig
from models.metadata import IngestionMetadata, ConfigurationMetadata, PipelineRunHistory

print("Initializing missing database tables...")
try:
    Base.metadata.create_all(bind=engine)
    print("Successfully initialized PipelineRunHistory and IngestionMetadata tables.")
except Exception as e:
    print(f"Failed to create tables: {e}")
    sys.exit(1)
