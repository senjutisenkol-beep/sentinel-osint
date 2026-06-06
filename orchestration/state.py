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

    # Agent 2 — Context Historian
    vector_context:     List[dict]
    graph_context:      List[str]
    temporal_context:   List[dict]
    historian_summary:  str
    context_confidence: float

    # Agent 3 — Threat Analyst
    threat_level:      str
    threat_score:      float
    threat_rationale:  str
    key_indicators:    List[str]
    threat_confidence: str

    # Agent 4 — Red Team
    red_team_assessment:  str
    counter_evidence:     List[str]
    revised_threat_score: float
    revised_confidence:   str