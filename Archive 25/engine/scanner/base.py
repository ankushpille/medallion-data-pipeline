import abc
from typing import Dict, List, Any

class CloudScanner(abc.ABC):
    """
    Abstract interface for Cloud Provider scanners.
    """
    @abc.abstractmethod
    def can_scan(self, settings: Any) -> bool:
        """Return True if required credentials exist in settings."""
        pass

    @abc.abstractmethod
    def scan(self, settings: Any, **kwargs) -> Dict[str, List[str]]:
        """
        Scan the cloud environment and return discoveries mapped by our schema:
        {
            "framework": [...],
            "source": [...],
            "ingestion": [...],
            "dq_rules": [...]
        }
        """
        pass
