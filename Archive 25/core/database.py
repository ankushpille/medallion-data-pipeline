from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from core.settings import settings

# Create Engine
# Fallback to local SQLite if no DB URL configured (Safety net for local dev)
db_url = settings.DATABASE_URL or "sqlite:///./dea_local.db"

# Normalize common URL shorthands so SQLAlchemy can find the correct dialect/driver.
# e.g. some tools provide a URL that begins with "postgres://" — map it to
# the explicit dialect plus driver: "postgresql+psycopg2://" which SQLAlchemy
# recognizes and which is compatible with psycopg2-binary installed in requirements.
if isinstance(db_url, str) and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)

engine = create_engine(db_url, connect_args={"check_same_thread": False} if "sqlite" in db_url else {})

# Create Session Factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base Class for Models
Base = declarative_base()

def get_db():
    """
    Dependency for FastAPI or Agents to get a DB session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
