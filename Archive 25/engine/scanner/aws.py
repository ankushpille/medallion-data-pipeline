import logging
from typing import Dict, List, Any
from .base import CloudScanner
from engine.analyzer.code_analyzer import CodeIntelligenceEngine

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None

logger = logging.getLogger("pipeline_ie.scanner.aws")

class AWSScanner(CloudScanner):
    def can_scan(self, settings: Any) -> bool:
        return bool(settings.aws_access_key_id and settings.aws_secret_access_key)

    def scan(self, settings: Any, **kwargs) -> Dict[str, List[str]]:
        raw_assets: Dict[str, List[str]] = {}
        
        if not boto3:
            logger.error("boto3 not installed.")
            return {"raw_cloud_dump": ["Boto3 missing"]}

        try:
            # 1. Establish initial session
            base_session = boto3.Session(
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name="us-east-1"
            )

            # 2. Discover all active global regions dynamically
            try:
                ec2 = base_session.client("ec2")
                regions = [region['RegionName'] for region in ec2.describe_regions()['Regions']]
            except Exception as e:
                logger.warning(f"Could not describe global regions, defaulting to us-east-1. Reason: {e}")
                regions = ["us-east-1"]

            # 3. Iterate over ALL regions exhaustively
            for region in regions:
                region_session = boto3.Session(
                    aws_access_key_id=settings.aws_access_key_id,
                    aws_secret_access_key=settings.aws_secret_access_key,
                    region_name=region
                )
                
                # S3 Discovery
                try:
                    s3 = region_session.client("s3")
                    if "s3" not in raw_assets: raw_assets["s3"] = []
                    if region == "us-east-1":
                        buckets = s3.list_buckets().get("Buckets", [])
                        for b in buckets:
                            b_name = b.get('Name')
                            # Deep S3 Inspection
                            config = {
                                "CreationDate": str(b.get('CreationDate', '')),
                                "StorageClass": "Standard",
                                "IsPublic": "Check Failure"
                            }
                            # Fetch Public Access Block Status
                            try:
                                pab = s3.get_public_access_block(Bucket=b_name).get('PublicAccessBlockConfiguration', {})
                                config["IsPublic"] = str(not all(pab.values()))
                                config["PublicAccessBlock"] = pab
                            except: pass
                            
                            # Fetch Policy Summary
                            try:
                                policy = s3.get_bucket_policy(Bucket=b_name).get('Policy')
                                if policy:
                                    config["HasPolicy"] = True
                                    config["PolicyLength"] = len(policy)
                            except: pass
                            
                            try:
                                v_status = s3.get_bucket_versioning(Bucket=b_name).get('Status', 'Disabled')
                                config["Versioning"] = v_status
                            except Exception: pass
                            
                            try:
                                enc = s3.get_bucket_encryption(Bucket=b_name).get('ServerSideEncryptionConfiguration', {})
                                config["Encryption"] = "Enabled" if enc else "Disabled"
                            except Exception: config["Encryption"] = "Disabled"

                            raw_assets["s3"].append({
                                "id": f"{region} || {b['Name']}",
                                "configuration": config
                            })
                except Exception: pass

                # API Gateway Discovery
                try:
                    apigw = region_session.client("apigateway")
                    if "apigateway" not in raw_assets: raw_assets["apigateway"] = []
                    apis = apigw.get_rest_apis().get("items", [])
                    for api in apis:
                        api_id = api['id']
                        source_urls = []
                        
                        try:
                            # Extract native REST Integration URLs to pinpoint data sources
                            resources = apigw.get_resources(restApiId=api_id).get('items', [])
                            api_methods = []
                            api_integrations = []
                            api_auth = "NONE"
                            
                            for res in resources:
                                res_path = res.get('path', '/')
                                for method, method_info in res.get('resourceMethods', {}).items():
                                    if method == 'OPTIONS': continue
                                    api_methods.append(method)
                                    
                                    # Auth check
                                    auth_type = method_info.get('authorizationType', 'NONE')
                                    if auth_type != 'NONE': api_auth = auth_type
                                    
                                    try:
                                        integ = apigw.get_integration(restApiId=api_id, resourceId=res['id'], httpMethod=method)
                                        # Track integrations
                                        integ_type = integ.get('type')
                                        integ_uri = integ.get('uri', '')
                                        api_integrations.append({
                                            "path": res_path,
                                            "method": method,
                                            "type": integ_type,
                                            "uri": integ_uri
                                        })
                                        if 'uri' in integ and 'amazonaws.com' not in integ['uri']:
                                            source_urls.append(integ['uri'])
                                    except Exception: pass
                        except Exception as e:
                            logger.debug(f"Integration extraction skipped for {api_id}: {e}")
                            
                        # Stage discovery
                        stages = []
                        try:
                            stages = [s['stageName'] for s in apigw.get_stages(restApiId=api_id).get('item', [])]
                        except Exception: pass

                        raw_assets["apigateway"].append({
                            "id": f"{region} || {api['name']}",
                            "configuration": {
                                "PublicInvokeURL": f"https://{api_id}.execute-api.{region}.amazonaws.com/prod",
                                "EndpointType": api.get('endpointConfiguration', {}).get('types', ['EDGE'])[0],
                                "Version": str(api.get('version', 'v1')),
                                "CreatedDate": str(api.get('createdDate', '')),
                                "SourceIntegrationURLs": source_urls if source_urls else ["Unknown/Internal"],
                                "Methods": list(set(api_methods)),
                                "Integrations": api_integrations,
                                "AuthType": api_auth,
                                "Stages": stages
                            }
                        })
                except Exception: pass

                # Lambda Deep Inspection
                try:
                    lambda_client = region_session.client("lambda")
                    if "lambda" not in raw_assets: raw_assets["lambda"] = []
                    funcs = lambda_client.list_functions().get("Functions", [])
                    analyzer = CodeIntelligenceEngine()

                    for f in funcs:
                        targets = []
                        code_evidence = []
                        confidence = "LOW"
                        
                        env = f.get("Environment", {}).get("Variables", {})
                        for k, v in env.items():
                            if "BUCKET" in k.upper() or "TABLE" in k.upper() or "QUEUE" in k.upper() or "URL" in k.upper():
                                targets.append(v)
                        
                        if targets: confidence = "MEDIUM"

                        # Deep Code AST Parse
                        findings = {}
                        try:
                            # Requires lambda:GetFunction perms. If we get the presigned URL, we analyze AST
                            fn_info = lambda_client.get_function(FunctionName=f['FunctionName'])
                            code_loc = fn_info.get('Code', {}).get('Location')
                            if code_loc:
                                findings = analyzer.analyze_presigned_url(code_loc)
                                if findings.get("targets"):
                                    targets.extend(findings["targets"])
                                    confidence = "HIGH"
                                if findings.get("evidence"):
                                    code_evidence.extend(findings["evidence"])
                        except Exception as ce:
                            logger.debug(f"Could not analyze lambda code for {f['FunctionName']}: {ce}")

                        # Fetch Event Source Mappings (Triggers)
                        event_triggers = []
                        try:
                            paginator = lambda_client.get_paginator('list_event_source_mappings')
                            for page in paginator.paginate(FunctionName=f['FunctionName']):
                                for mapping in page.get('EventSourceMappings', []):
                                    src_arn = mapping.get('EventSourceArn', '')
                                    if src_arn:
                                        trigger_type = src_arn.split(':')[2].upper()
                                        event_triggers.append(trigger_type)
                        except Exception: pass

                        raw_assets["lambda"].append({
                            "id": f"{region} || {f['FunctionName']}",
                            "env_targets": list(set(targets)),
                            "code_evidence": code_evidence,
                            "confidence": confidence,
                            "configuration": {
                                "Runtime": f.get("Runtime", "unknown"),
                                "MemorySizeMB": f.get("MemorySize", 128),
                                "TimeoutSeconds": f.get("Timeout", 3),
                                "Handler": f.get("Handler", ""),
                                "EphemeralStorage": f.get("EphemeralStorage", {}).get("Size", 512),
                                "Architecture": f.get("Architectures", ["x86_64"])[0],
                                "EnvironmentKeys": list(env.keys()),
                                "IngestionTargets": list(set(targets)) if targets else ["S3 Storage Layer (Heuristic mapping)"],
                                "IngestionOperations": list(set(findings.get("operations", []))) if findings.get("operations") else ["Unknown"],
                                "DataFormats": list(set(findings.get("data_formats", []))) if findings.get("data_formats") else ["Unknown"],
                                "VerifiedTriggers": list(set(event_triggers))
                            }
                        })
                except Exception: pass
                
                # Glue Deep Inspection
                try:
                    glue = region_session.client("glue")
                    if "glue" not in raw_assets: raw_assets["glue"] = []
                    jobs = glue.get_jobs().get("Jobs", [])
                    for j in jobs:
                        raw_assets["glue"].append({
                            "id": f"{region} || {j['Name']}",
                            "configuration": {
                                "GlueVersion": j.get("GlueVersion", "1.0"),
                                "MaxCapacity": j.get("MaxCapacity", 10.0),
                                "TimeoutMinutes": j.get("Timeout", 2880),
                                "CommandScript": j.get("Command", {}).get("ScriptLocation", "Unknown")
                            }
                        })
                except Exception as e: pass

                # General Tagged Discovery Fallback
                try:
                    tagging_client = region_session.client('resourcegroupstaggingapi')
                    for page in tagging_client.get_paginator('get_resources').paginate():
                        for resource in page.get('ResourceTagMappingList', []):
                            arn = resource.get('ResourceARN', '')
                            parts = arn.split(':')
                            if len(parts) >= 3:
                                service = parts[2]
                                if service not in raw_assets:
                                    raw_assets[service] = []
                                if len(raw_assets[service]) < 50:
                                    identifier = parts[-1].split('/')[-1] if '/' in parts[-1] else parts[-1]
                                    # Fallback uses strings to prevent breaking, but structured services use dicts
                                    raw_assets[service].append({"id": f"{region} || {identifier}"})
                except Exception as e:
                    logger.debug(f"Resource Tagging API failed in {region}: {e}")

        except Exception as e:
            logger.error(f"Global AWS Scan failed: {e}")
            
        return {"raw_cloud_dump": [raw_assets]}
