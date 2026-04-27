import asyncio
import logging
from typing import Dict, List, Any
from starlette.concurrency import run_in_threadpool


from .aws import AWSScanner

logger = logging.getLogger("pipeline_ie.scanner.manager")

class ScannerManager:
    def __init__(self):
        # Register available scanners
        from .azure import AzureScanner
        from .gcp import GCPScanner
        from .fabric import FabricScanner
        
        self.scanners = [
            AWSScanner(),
            AzureScanner(),
            GCPScanner(),
            FabricScanner()
        ]
        self._scanner_map = {
            "aws": AWSScanner,
            "azure": AzureScanner,
            "gcp": GCPScanner,
            "fabric": FabricScanner
        }

    async def scan_all(self, settings: Any, providers: List[str] | None = None, **kwargs) -> Dict[str, List[str]]:
        logger.info(f"Scan requested for providers: {providers}")
        aggregated = {
            "framework": [],
            "source": [],
            "ingestion": [],
            "dq_rules": []
        }
        
        async def _run_scanner(scanner):
            name = scanner.__class__.__name__
            p_key = name.lower().replace('scanner', '')
            
            # 1. Strict Provider Filtering
            if providers:
                # Clean and normalize requested providers (e.g. ['azure', ''])
                requested = [p.strip().lower() for p in providers if p.strip()]
                if requested and p_key not in requested:
                    logger.info(f"Skipping {name} (not in selection: {requested})")
                    return None

            # 2. Credential Check
            if not scanner.can_scan(settings):
                logger.warning(f"Skipping {name} (missing credentials for {name})")
                return None

            # 3. Execution
            logger.info(f"Triggering {name} discovery...")
            try:
                # Run sync scanner in thread pool to avoid blocking event loop
                return await run_in_threadpool(scanner.scan, settings, **kwargs)
            except Exception as e:
                logger.error(f"Discovery failed for {name}: {str(e)}", exc_info=True)
            return None

        # Launch all scanners in parallel
        tasks = [_run_scanner(s) for s in self.scanners]
        results = await asyncio.gather(*tasks)
        
        # Aggregate results
        for res in results:
            if not res: continue
            for k, v in res.items():
                if k not in aggregated:
                    aggregated[k] = []
                
                for item in v:
                    if item not in aggregated[k]:
                        aggregated[k].append(item)
                    
        return aggregated

def _make_scanner_manager() -> ScannerManager:
    return ScannerManager()


# Lazy singleton — created on first access so imports never block
_scanner_manager_instance: ScannerManager | None = None


def get_scanner_manager() -> ScannerManager:
    global _scanner_manager_instance
    if _scanner_manager_instance is None:
        _scanner_manager_instance = _make_scanner_manager()
    return _scanner_manager_instance


# Backwards-compatible alias (used by api/main.py)
scanner_manager = get_scanner_manager()
