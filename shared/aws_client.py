"""
Centralised AWS client factory for Sentinel-OSINT.

Engineering note — why a factory instead of boto3.client() everywhere?
  1. Lambda reuse: clients created outside the handler are reused across
     warm invocations (connection pooling). We cache them here.
  2. Testability: tests can patch get_client() to return mocks without
     monkey-patching boto3 globally.
  3. Config isolation: region and profile live in config.py, not scattered
     across files.
  4. Retry config: all clients share the same botocore retry strategy,
     set once here.
"""
import boto3
from botocore.config import Config
from config import AWS_REGION, AWS_PROFILE

# Standard retry config for all clients.
# mode="adaptive" backs off exponentially and respects Retry-After headers.
_RETRY_CONFIG = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 3, "mode": "adaptive"},
)

# Module-level cache: service_name → client instance
_CLIENT_CACHE: dict = {}


def get_client(service: str) -> boto3.client:
    """
    Return a cached boto3 client for the given AWS service.

    Clients are created once and reused. Thread-safety is not a concern
    in Lambda (single-threaded per invocation), but if you move logic to
    ECS/EC2 you'll want a lock around this cache.
    """
    if service not in _CLIENT_CACHE:
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
        _CLIENT_CACHE[service] = session.client(service, config=_RETRY_CONFIG)
    return _CLIENT_CACHE[service]


# ── Convenience accessors ────────────────────────────────────────────────────
# Import these directly rather than calling get_client() each time.

def s3():
    return get_client("s3")

def bedrock_agent_runtime():
    """
    bedrock-agent-runtime: used to INVOKE agents (InvokeAgent API).
    Distinct from bedrock-agent (management API for creating agents).
    """
    return get_client("bedrock-agent-runtime")

def bedrock_agent():
    """bedrock-agent: management API for creating/listing/updating agents."""
    return get_client("bedrock-agent")

def bedrock_runtime():
    """bedrock-runtime: direct model invocation (InvokeModel API)."""
    return get_client("bedrock-runtime")

def stepfunctions():
    return get_client("stepfunctions")

def cloudwatch():
    return get_client("cloudwatch")

def iam():
    return get_client("iam")
