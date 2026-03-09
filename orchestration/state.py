from typing import TypedDict, List

class SentinelState(TypedDict):

    # Pipeline-level — set once, never changed
    session_id:        str
    analyst_query:     str
    retrieved_at:      str

    # Agent 1 — Signal Monitor
    gdelt_events:      List[dict]
    signal_confidence: float
    signal_summary:    str

    # Pipeline control
    abort:             bool
    abort_reason:      str

    # Error tracking
    errors:            List[str]