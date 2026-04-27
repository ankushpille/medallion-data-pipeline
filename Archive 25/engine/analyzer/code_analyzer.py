import ast
import requests
import zipfile
import io
import logging
from typing import List, Dict, Any

logger = logging.getLogger("pipeline_ie.analyzer.code_analyzer")

class CodeIntelligenceEngine:
    """
    Parses code directly (via AST where possible) to extract exact
    dependencies, targets, operations, and structure evidence.
    """
    
    def analyze_presigned_url(self, url: str) -> Dict[str, Any]:
        """Download, extract and analyze python files from a deployment package."""
        findings = {
            "targets": [],
            "operations": [],
            "data_formats": [],
            "evidence": []
        }
        
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"Could not download code payload, status: {resp.status_code}")
                return findings
                
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                # Find python files
                py_files = [n for n in z.namelist() if n.endswith('.py')]
                for p_file in py_files:
                    with z.open(p_file) as f:
                        code_str = f.read().decode('utf-8')
                        file_findings = self.analyze_python_code(code_str)
                        
                        findings["targets"].extend(file_findings["targets"])
                        findings["operations"].extend(file_findings["operations"])
                        findings["data_formats"].extend(file_findings["data_formats"])
                        findings["evidence"].extend(file_findings["evidence"])

        except Exception as e:
            logger.error(f"Failed to analyze code package: {e}")
            
        # Deduplicate while preserving order using dict
        findings["targets"] = list(dict.fromkeys(findings["targets"]))
        findings["operations"] = list(dict.fromkeys(findings["operations"]))
        findings["data_formats"] = list(dict.fromkeys(findings["data_formats"]))
        findings["evidence"] = list(dict.fromkeys(findings["evidence"]))
        return findings

    def analyze_python_code(self, source_code: str) -> Dict[str, Any]:
        findings = {
            "targets": [],
            "operations": [],
            "data_formats": [],
            "evidence": []
        }
        try:
            tree = ast.parse(source_code)
        except SyntaxError:
            # Code might not be valid py3, fallback gracefully
            return findings

        # Basic AST pass
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                # Look for object.method calls
                if isinstance(func, ast.Attribute):
                    method_name = func.attr
                    
                    # Boto3 operation mapping
                    if method_name in ('put_object', 'upload_file', 'put_record', 'put_item'):
                        findings["operations"].append("write")
                    elif method_name in ('get_object', 'download_file', 'query', 'scan', 'invoke'):
                        findings["operations"].append("read")
                        
                    # Target extraction mapping (S3 Buckets)
                    if method_name in ('put_object', 'get_object'):
                        for kw in node.keywords:
                            if kw.arg == 'Bucket' and isinstance(kw.value, ast.Constant):
                                tgt = f"s3://{kw.value.value}"
                                findings["targets"].append(tgt)
                                findings["evidence"].append(f"AST Code Map: Detected `{method_name}` targeting `{tgt}`")
                                
                    # SNS / SQS
                    if method_name in ('publish', 'send_message'):
                        for kw in node.keywords:
                            if kw.arg in ('TopicArn', 'QueueUrl') and isinstance(kw.value, ast.Constant):
                                tgt = str(kw.value.value)
                                findings["targets"].append(tgt)
                                findings["evidence"].append(f"AST Code Map: Detected messaging via `{method_name}` to `{tgt}`")
                                
            # Detect data formats via imports
            if isinstance(node, ast.Import):
                for name in node.names:
                    if name.name in ('json', 'csv', 'xml', 'yaml', 'avro'):
                        findings["data_formats"].append(name.name)
            if isinstance(node, ast.ImportFrom):
                if node.module in ('json', 'csv', 'yaml'):
                    findings["data_formats"].append(node.module)
                        
        return findings
