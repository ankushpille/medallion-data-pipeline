"""
URN/ARN parser module for the Pipeline Intelligence Engine.

Handles parsing of DataHub URNs and AWS ARNs into structured components,
and supports round-trip re-serialisation of URNs.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedURN:
    """
    Structured representation of a DataHub URN.

    Supports three URN types:
    - dataJob:  urn:li:dataJob:(urn:li:dataFlow:(platform,flow_id,env),job_id)
    - dataFlow: urn:li:dataFlow:(platform,flow_id,env)
    - dataset:  urn:li:dataset:(urn:li:dataPlatform:platform,name,env)
    """

    platform: str
    environment: str
    flow_id: Optional[str]
    job_id: Optional[str]
    raw: str

    def to_urn(self) -> str:
        """
        Round-trip re-serialisation back to a DataHub URN string.

        - dataJob  (flow_id and job_id both set):
            urn:li:dataJob:(urn:li:dataFlow:(platform,flow_id,environment),job_id)
        - dataFlow (flow_id set, job_id None):
            urn:li:dataFlow:(platform,flow_id,environment)
        - dataset  (flow_id set as dataset name, job_id None, detected via raw):
            urn:li:dataset:(urn:li:dataPlatform:platform,flow_id,environment)
        """
        if self.job_id is not None and self.flow_id is not None:
            # dataJob URN
            return (
                f"urn:li:dataJob:(urn:li:dataFlow:"
                f"({self.platform},{self.flow_id},{self.environment}),{self.job_id})"
            )
        if self.raw.startswith("urn:li:dataset:"):
            # dataset URN — flow_id holds the dataset name
            name = self.flow_id if self.flow_id is not None else ""
            return (
                f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{name},{self.environment})"
            )
        # dataFlow URN
        flow = self.flow_id if self.flow_id is not None else ""
        return f"urn:li:dataFlow:({self.platform},{flow},{self.environment})"


@dataclass
class ParsedARN:
    """
    Structured representation of an AWS ARN.

    AWS ARN format: arn:partition:service:region:account-id:resource
    """

    service: str
    region: str
    account_id: str
    resource_id: str
    raw: str


# ---------------------------------------------------------------------------
# Internal regex patterns
# ---------------------------------------------------------------------------

# urn:li:dataJob:(urn:li:dataFlow:(platform,flow_id,env),job_id)
_DATA_JOB_RE = re.compile(
    r"^urn:li:dataJob:\("
    r"urn:li:dataFlow:\(([^,]+),([^,]+),([^)]+)\)"
    r",([^)]+)\)$"
)

# urn:li:dataFlow:(platform,flow_id,env)
_DATA_FLOW_RE = re.compile(
    r"^urn:li:dataFlow:\(([^,]+),([^,]+),([^)]+)\)$"
)

# urn:li:dataset:(urn:li:dataPlatform:platform,name,env)
_DATASET_RE = re.compile(
    r"^urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),([^)]+)\)$"
)


def parse_urn(urn: str) -> Optional[ParsedURN]:
    """
    Parse a DataHub URN string into its components.

    Handles these URN formats:
    - urn:li:dataJob:(urn:li:dataFlow:(platform,flow_id,env),job_id)
    - urn:li:dataFlow:(platform,flow_id,env)
    - urn:li:dataset:(urn:li:dataPlatform:platform,name,env)

    Returns None for malformed/unrecognised URNs.
    """
    if not isinstance(urn, str) or not urn:
        return None

    # Try dataJob first (most specific)
    m = _DATA_JOB_RE.match(urn)
    if m:
        platform, flow_id, environment, job_id = m.groups()
        return ParsedURN(
            platform=platform,
            environment=environment,
            flow_id=flow_id,
            job_id=job_id,
            raw=urn,
        )

    # Try dataFlow
    m = _DATA_FLOW_RE.match(urn)
    if m:
        platform, flow_id, environment = m.groups()
        return ParsedURN(
            platform=platform,
            environment=environment,
            flow_id=flow_id,
            job_id=None,
            raw=urn,
        )

    # Try dataset
    m = _DATASET_RE.match(urn)
    if m:
        platform, name, environment = m.groups()
        return ParsedURN(
            platform=platform,
            environment=environment,
            flow_id=name,   # dataset name stored in flow_id slot
            job_id=None,
            raw=urn,
        )

    return None


def parse_arn(arn: str) -> Optional[ParsedARN]:
    """
    Parse an AWS ARN string into its components.

    AWS ARN format: arn:partition:service:region:account-id:resource
    Examples:
    - arn:aws:glue:us-east-1:123456789012:job/my_job
    - arn:aws:lambda:us-east-1:123456789012:function:MyFunction
    - arn:aws:s3:::my-bucket
    - arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-XXXXXXXX

    Returns None for malformed ARNs.
    """
    if not isinstance(arn, str) or not arn:
        return None

    parts = arn.split(":")
    # Minimum valid ARN: arn:partition:service:region:account:resource
    # S3 ARNs can have empty region/account: arn:aws:s3:::bucket
    if len(parts) < 6:
        return None

    prefix = parts[0]
    if prefix != "arn":
        return None

    # parts[1] = partition, parts[2] = service
    service = parts[2]
    if not service:
        return None

    region = parts[3]       # may be empty (e.g. S3)
    account_id = parts[4]   # may be empty (e.g. S3)

    # Resource is everything from index 5 onward, rejoined with ":"
    resource_id = ":".join(parts[5:])
    if not resource_id:
        return None

    return ParsedARN(
        service=service,
        region=region,
        account_id=account_id,
        resource_id=resource_id,
        raw=arn,
    )
