import hashlib
from typing import Tuple

def generate_dataset_id(client_name: str, source_type: str, file_path: str) -> str:
    """
    Standardized Dataset ID generation across the entire platform.
    Uses case-insensitive hashing of Client + Source Type + Path for high reliability.
    """
    client = client_name.lower().strip()
    src = source_type.upper().strip()
    path = file_path.lower().strip().replace(" ", "_")
    
    # Combined string: e.g. "lc1LOCALanalytics_data_2.csv"
    raw = f"{client}{src}{path}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def parse_s3_url(url: str) -> Tuple[str, str]:
    """
    Parses S3 URLs in multiple formats:
      1. s3://bucket/key
      2. s3://bucket
      3. https://bucket.s3.region.amazonaws.com/key
      4. https://s3.region.amazonaws.com/bucket/key
    
    Returns (bucket, key).
    """
    url = url.strip().replace("s3:// ", "s3://")
    
    if url.startswith("s3://"):
        rest = url[len("s3://"):]
        parts = rest.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    if "amazonaws.com" in url and "s3" in url:
        # Format 3: https://bucket.s3.region.amazonaws.com/key
        # OR Format 4: https://s3.region.amazonaws.com/bucket/key
        from urllib.parse import urlparse
        parsed = urlparse(url)
        netloc = parsed.netloc # bucket.s3.region.amazonaws.com OR s3.region.amazonaws.com
        path = parsed.path.lstrip("/") # bucket/key OR key
        
        if netloc.startswith("s3."):
            # Format 4
            parts = path.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        else:
            # Format 3
            bucket = netloc.split(".s3", 1)[0]
            key = path
        return bucket, key

    # Fallback: assume bucket/key
    parts = url.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key
