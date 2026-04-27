from typing import Dict, Any
import pandas as pd
from loguru import logger

class ValidationService:
    @staticmethod
    def validate_file(storage_client, container: str, key: str) -> Dict[str, Any]:
        """
        Validate file existence, size, and format.
        """
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
        except Exception:
            raise ValueError(f"File does not exist: s3://{bucket}/{key}")

        if response["ContentLength"] == 0:
            raise ValueError("File is empty (0 bytes)")

        if not key.lower().endswith((".csv", ".json", ".parquet")):
            raise ValueError(f"Unsupported file format: {key}")

        return response

    @staticmethod
    def validate_content(content: bytes, filename: str, expected_size: int = None) -> bool:
        """
        Validate in-memory content (for Ingestion Service)
        """
        content_size = len(content)
        if content_size == 0:
            raise ValueError("File content is empty (0 bytes)")

        if expected_size is not None and expected_size > 0:
            # Allow 20% tolerance — API sources estimate size before download
            # so actual bytes often differ slightly from expected
            lower = expected_size * 0.5
            upper = expected_size * 2.0
            if content_size == 0:
                raise ValueError(f"Incomplete download: Expected {expected_size} bytes but got 0 bytes")
            if content_size < lower:
                raise ValueError(
                    f"Incomplete download: Expected ~{expected_size} bytes, "
                    f"but only got {content_size} bytes (less than 50% of expected)"
                )
            # If actual is LARGER than expected that is fine — API may return more data

        filename_lower = filename.lower()
        
        # 1. Extension Check
        if not filename_lower.endswith((".csv", ".json", ".parquet")):
            raise ValueError(f"Unsupported file format: {filename}")

        # 2. Structure/Content Check
        import io
        
        if filename_lower.endswith(".csv"):
            try:
                # Attempt to read with pandas to check structure
                # Use io.BytesIO to read bytes as file
                df = pd.read_csv(io.BytesIO(content))
                
                # Check for empty dataframe (no rows)
                if df.empty:
                    raise ValueError("CSV file is empty (contains header but no data)")
                    
                # Check for no columns
                if df.columns.empty:
                    raise ValueError("CSV file has no columns")
                    
                # Check for duplicates
                if df.columns.duplicated().any():
                    raise ValueError("Duplicate column names found in CSV")

                 # Check for "Unnamed" columns (indicates missing header)
                for col in df.columns:
                    if "Unnamed" in str(col) or str(col).strip() == "":
                        logger.error(f"Malformed CSV header in {filename}: Found '{col}'")
                        raise ValueError(f"Invalid column name found: '{col}'. Check for missing headers or malformed CSV.")
                
                logger.info(f"CSV Validation SUCCESS for {filename}: {len(df)} rows, {len(df.columns)} columns identified.")
                    
            except pd.errors.EmptyDataError:
                logger.error(f"CSV Empty Error for {filename}")
                raise ValueError("CSV file is empty or contains no columns")
            except pd.errors.ParserError as e:
                logger.error(f"CSV Parsing Error for {filename}: {e}")
                raise ValueError(f"CSV Parsing Error: {str(e)}")
            except Exception as e:
                # Re-raise explicit validation errors
                if "ValueError" in str(type(e)):
                     raise e
                logger.error(f"CSV structure check failed for {filename}: {e}")
                raise ValueError(f"Failed to validate CSV content: {str(e)}")

        elif filename_lower.endswith(".json"):
             try:
                df = pd.read_json(io.BytesIO(content))
                if df.empty:
                    logger.error(f"JSON Empty Error for {filename}")
                    raise ValueError("JSON file is empty")
                logger.info(f"JSON Validation SUCCESS for {filename}: {len(df)} records found.")
             except ValueError as e:
                 logger.error(f"JSON Parsing Error for {filename}: {e}")
                 raise ValueError(f"Invalid JSON format: {str(e)}")
        
        return True

    @staticmethod
    def validate_structure(df: pd.DataFrame):
        """
        Validate basic dataset structure (columns, duplication)
        """
        if df.columns.empty:
            raise ValueError("Dataset has no columns")

        if df.columns.duplicated().any():
            raise ValueError("Duplicate column names found")

        # Check for empty or "Unnamed" columns
        for col in df.columns:
            if "Unnamed" in str(col) or str(col).strip() == "":
                raise ValueError(f"Invalid column name found: '{col}'. Check for missing headers.")
