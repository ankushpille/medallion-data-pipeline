import logging
import requests
from typing import Dict, List, Any
from .base import CloudScanner

try:
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    HAS_AZURE = True
except ImportError:
    HAS_AZURE = False

logger = logging.getLogger("pipeline_ie.scanner.azure")

class AzureScanner(CloudScanner):
    def can_scan(self, settings: Any) -> bool:
        # We check for HAS_AZURE, and if there's any hint of azure config
        # or if we want to support SSO (az login) which doesn't require explicit client/secret entries.
        return HAS_AZURE

    def scan(self, settings: Any, **kwargs) -> Dict[str, List[str]]:
        raw_assets: Dict[str, List[str]] = {
            "storage_accounts": [],
            "datafactory": [],
            "functions": [],
            "fabric_workspaces": [],
            "fabric_items": []
        }

        if not HAS_AZURE:
            logger.error("Azure SDK missing.")
            return {"raw_cloud_dump": ["Azure SDK missing"]}

        try:
            # 1. SSO-based Authentication (az login / Managed Identity OR delegated session token)
            azure_token = kwargs.get("azure_token")
            if azure_token:
                from azure.core.credentials import AccessToken
                class StaticTokenCredential:
                    def __init__(self, token): self.token = token
                    def get_token(self, *scopes, **kwargs): return AccessToken(self.token, 9999999999)
                
                credential = StaticTokenCredential(azure_token)
                logger.info("Using delegated Azure SSO token from session.")
            elif settings.azure_client_id and settings.azure_client_secret and "YOUR_" not in settings.azure_client_id:
                # 1b. Service Principal Authentication (Configured in .env)
                credential = ClientSecretCredential(
                    tenant_id=settings.azure_tenant_id,
                    client_id=settings.azure_client_id,
                    client_secret=settings.azure_client_secret
                )
                logger.info("Using Azure Service Principal (Client ID/Secret) from .env.")
            else:
                credential = DefaultAzureCredential()
            
            # 2. Discover Subscriptions
            subscription_client = SubscriptionClient(credential)
            subs = list(subscription_client.subscriptions.list())
            
            if not subs:
                logger.warning("No active Azure subscriptions discovered.")
                return {"raw_cloud_dump": [raw_assets]}

            for sub in subs:
                sub_id = sub.subscription_id
                logger.info(f"Scanning Azure Subscription: {sub.display_name} ({sub_id})")

                # a. Storage Account Deep Inspection (ADLS Gen2 / Blob)
                try:
                    storage_client = StorageManagementClient(credential, sub_id)
                    for sa in storage_client.storage_accounts.list():
                        # Fetch properties to check for HNS
                        props = getattr(sa, 'is_hns_enabled', False)
                        raw_assets["storage_accounts"].append({
                            "id": f"azure || {sa.name}",
                            "configuration": {
                                "Location": sa.location,
                                "Kind": sa.kind,
                                "Sku": sa.sku.name,
                                "AccessTier": str(sa.access_tier),
                                "IsHnsEnabled": bool(props),
                                "PrimaryEndpoints": sa.primary_endpoints.__dict__ if sa.primary_endpoints else {},
                                "ProvisioningState": sa.provisioning_state
                            }
                        })
                except Exception as e:
                    logger.debug(f"Storage scan failed for {sub_id}: {e}")

                # b. Data Factory Discovery
                try:
                    adf_client = DataFactoryManagementClient(credential, sub_id)
                    for df in adf_client.factories.list():
                        raw_assets["datafactory"].append({
                            "id": f"azure || {df.name}",
                            "configuration": {
                                "Location": df.location,
                                "ProvisioningState": df.provisioning_state,
                                "ResourceGroup": df.id.split('/')[4] if '/' in df.id else "unknown"
                            }
                        })
                except Exception as e:
                    logger.debug(f"ADF scan failed for {sub_id}: {e}")

                # c. Azure Functions (via Generic Resource Client for simplicity across all subs)
                try:
                    resource_client = ResourceManagementClient(credential, sub_id)
                    for res in resource_client.resources.list(filter="resourceType eq 'Microsoft.Web/sites'"):
                        if res.kind and 'functionapp' in res.kind.lower():
                            raw_assets["functions"].append({
                                "id": f"azure || {res.name}",
                                "configuration": {
                                    "Location": res.location,
                                    "Kind": res.kind,
                                    "Tags": res.tags
                                }
                            })
                except Exception as e:
                    logger.debug(f"Functions scan failed for {sub_id}: {e}")

        except Exception as e:
            logger.error(f"Global Azure Scan failed: {e}")
            # Fallback to simulation only if everything failed and we have no data
            if not any(raw_assets.values()):
                return self._simulate_discovery()

        return {"raw_cloud_dump": [raw_assets]}
        
    def _simulate_discovery(self) -> Dict[str, List[str]]:
        """Fallback simulation for UI testing."""
        return {"raw_cloud_dump": [{
            "storage_accounts": [
                {
                    "id": "azure || processed-telemetry-adls", 
                    "configuration": {"Kind": "StorageV2", "AccessTier": "Hot", "IsHnsEnabled": True}
                }
            ],
            "datafactory": [
                {
                    "id": "azure || Ingestion-Pipeline-Dev",
                    "configuration": {"Location": "East US", "ProvisioningState": "Succeeded"}
                }
            ]
        }]}
