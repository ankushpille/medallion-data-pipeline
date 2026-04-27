#!/usr/bin/env python3
"""Create all tables that are defined in models.*"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.database import engine, Base
# Import all models so they register with Base.metadata
import models.master_config_authoritative
import models.dq_schema_config

if __name__ == "__main__":
    print("Creating tables...")
    Base.metadata.create_all(bind=engine)
    print("Done.")