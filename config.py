"""
Central configuration for Sentinel-OSINT.

All environment-dependent values live here. Import this module everywhere
instead of reading os.environ directly — it gives us a single place to
validate config at startup and makes testing easy (just mock this module).
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── AWS ──────────────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_PROFILE = os.getenv("AWS_PROFILE", "senjutisen")
ENV = os.getenv("ENV", "dev")

# ── S3 ───────────────────────────────────────────────────────────────────────
SENTINEL_BUCKET = os.getenv("SENTINEL_BUCKET", f"sentinel-osint-{ENV}")

# S3 key prefixes — all agent I/O flows through these paths
S3_PREFIX_RAW_SIGNALS     = "signals/raw"
S3_PREFIX_PROCESSED       = "signals/processed"
S3_PREFIX_CONTEXT         = "analysis/context"
S3_PREFIX_ASSESSMENTS     = "analysis/assessments"
S3_PREFIX_RED_TEAM        = "analysis/red-team"
S3_PREFIX_REPORTS         = "reports"
S3_PREFIX_KB_DOCUMENTS    = "kb-documents"

# ── Bedrock ───────────────────────────────────────────────────────────────────
# Claude Sonnet 4.6 — strong reasoning, reasonable cost for multi-agent pipelines
BEDROCK_MODEL_ID = os.getenv(
    "BEDROCK_MODEL_ID",
    "anthropic.claude-sonnet-4-6-20251001-v1:0"
)

# ── Agent IDs (populated after first deployment) ─────────────────────────────
SIGNAL_MONITOR_AGENT_ID          = os.getenv("SIGNAL_MONITOR_AGENT_ID", "")
SIGNAL_MONITOR_AGENT_ALIAS_ID    = os.getenv("SIGNAL_MONITOR_AGENT_ALIAS_ID", "TSTALIASID")
CONTEXT_HISTORIAN_AGENT_ID       = os.getenv("CONTEXT_HISTORIAN_AGENT_ID", "")
CONTEXT_HISTORIAN_AGENT_ALIAS_ID = os.getenv("CONTEXT_HISTORIAN_AGENT_ALIAS_ID", "TSTALIASID")
THREAT_ANALYST_AGENT_ID          = os.getenv("THREAT_ANALYST_AGENT_ID", "")
THREAT_ANALYST_AGENT_ALIAS_ID    = os.getenv("THREAT_ANALYST_AGENT_ALIAS_ID", "TSTALIASID")
RED_TEAM_AGENT_ID                = os.getenv("RED_TEAM_AGENT_ID", "")
RED_TEAM_AGENT_ALIAS_ID          = os.getenv("RED_TEAM_AGENT_ALIAS_ID", "TSTALIASID")

# ── Knowledge Base ────────────────────────────────────────────────────────────
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID", "")

# ── Step Functions ────────────────────────────────────────────────────────────
WORKFLOW_STATE_MACHINE_ARN = os.getenv("WORKFLOW_STATE_MACHINE_ARN", "")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Risk Scoring ──────────────────────────────────────────────────────────────
# Threat Analyst uses these dimension weights to compute an overall risk score.
# They sum to 1.0. Adjust based on domain knowledge.
RISK_DIMENSION_WEIGHTS = {
    "military_escalation":  0.30,
    "economic_disruption":  0.25,
    "diplomatic_breakdown": 0.20,
    "information_warfare":  0.15,
    "humanitarian_impact":  0.10,
}

# Score thresholds for alert tiers
RISK_THRESHOLD_CRITICAL = 75
RISK_THRESHOLD_HIGH     = 55
RISK_THRESHOLD_MEDIUM   = 35
