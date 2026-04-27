import pandas as pd
from io import BytesIO
from sqlalchemy.orm import Session
from core.database import SessionLocal, engine
from models.master_config import MasterConfig
from core.settings import settings
from core.azure_storage import get_storage_client
from loguru import logger

def export_master_config_to_storage(bucket_name: str = "ag-de-agent", client_name: str = None):
    """
    Reads the MasterConfig table (optionally filtered by client)
    and saves it as an Excel file in Azure Blob Storage at:
    Master_Configuration/{Client_Name}/master_config.xlsx
    
    If client_name is None, it exports ALL? Or logs warning? 
    Given the new requirement, we likely always want client_name.
    But for safety, if None, we might dump to legacy or skip.
    """
    session: Session = SessionLocal()
    try:
        query = session.query(MasterConfig)
        
        if client_name:
            query = query.filter(MasterConfig.client_name == client_name)
            s3_key = f"Master_Configuration/{client_name}/master_config.xlsx"
        else:
            # Fallback for legacy calls or bulk export (not requested but good safety)
            # Or raise error? Let's default to legacy if no client specified
            s3_key = "config/master_config.xlsx" 
        
        df = pd.read_sql(query.statement, session.bind)
        
        if df.empty:
            logger.warning(f"Master Config Table is empty for client {client_name}. Skipping export.")
            return

        # Azure Storage Client
        storage = get_storage_client()

        with BytesIO() as output:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)

            storage.put_object(
                Container=bucket_name,
                Key=s3_key,
                Body=output.read(),
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        logger.info(f"Exported Master Config to az://{bucket_name}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Failed to export Master Config to S3: {e}")
    finally:
        session.close()
