"""
Structured logging for all Sentinel-OSINT components.

Why structured logging (JSON) over print() or stdlib logging?
  - CloudWatch Logs Insights can query JSON fields with SQL-like syntax
  - Every log line carries a trace_id linking all events for one workflow run
  - Log parsing tools (Splunk, Datadog) consume JSON natively
  - Structured fields make it trivial to filter by agent, run_id, risk_score, etc.

This uses structlog, which wraps stdlib logging and adds a processing pipeline
that serialises log records to JSON with guaranteed fields.
"""
import sys
import structlog
import logging
from config import LOG_LEVEL


def configure_logging() -> None:
    """Call once at Lambda cold start or script entry point."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


def get_logger(name: str, **context) -> structlog.stdlib.BoundLogger:
    """
    Return a logger bound with permanent context fields.

    Usage:
        log = get_logger("signal_monitor", run_id="abc-123", agent="signal_monitor")
        log.info("signal_parsed", entity_count=5, event_type="military")
    """
    configure_logging()
    return structlog.get_logger(name).bind(**context)
