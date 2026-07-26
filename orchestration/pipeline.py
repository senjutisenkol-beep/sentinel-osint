# ─────────────────────────────────────────────────────────────────────────────
# orchestration/pipeline.py
#
# This is the central nervous system of the Sentinel-OSINT pipeline.
# It defines every agent node as a Python function, then wires them together
# into a LangGraph StateGraph — a directed graph where each node is an agent
# and each edge is a handoff between agents.
#
# Current pipeline shape (Day 8):
#   signal_monitor ──► context_historian ──► END
#                 └──► clarification ──► END  (if confidence too low)
#
# Week 3 will extend this to:
#   signal_monitor ──► context_historian ──► threat_analyst ──► red_team ──► END
# ─────────────────────────────────────────────────────────────────────────────



import os
from dotenv import load_dotenv
load_dotenv()

import json
import boto3

# StateGraph is LangGraph's graph builder — it lets us register nodes and edges.
# END is LangGraph's built-in terminal marker — any edge pointing to END
# means "this is the last step, stop the pipeline here."
from langgraph.graph import StateGraph, END

# SentinelState is our shared state TypedDict — the "briefing document" that
# every agent reads from and writes to. Defined in orchestration/state.py.
from orchestration.state import SentinelState

# The three retrieval functions from Agent 2's retriever module.
from agents.context_historian.retriever import (
    graph_retrieve,
    temporal_retrieve,
    vector_retrieve
)

# The synthesiser calls Claude directly via bedrock-runtime to produce
# a single intelligence summary and a confidence score.
from agents.context_historian.synthesizer import synthesise
from agents.threat_analyst.threat_analyst import assess_threat
from agents.red_team.red_team import challenge_assessment
from agents.flash_report.flash_report import generate_flash_report


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING — SHARED PREDICATES
# ─────────────────────────────────────────────────────────────────────────────

def should_red_team(state: dict) -> bool:
    """
    Single source of truth for whether Agent 4 fires.

    Agent 4 challenges assessments that CLAIM danger.
    The claim lives in threat_level (Claude's analytical
    judgment), not in threat_score (the arithmetic
    aggregate). A HIGH label below the score threshold
    is exactly the case that most needs challenging.
    """
    return (
        state.get('threat_level', '') == 'HIGH'
        or state.get('threat_score', 0.0) >= 0.7
    )


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 1 — SIGNAL MONITOR NODE
# ─────────────────────────────────────────────────────────────────────────────

def _refine_query(query: str, attempt: int) -> str:
    """Broadens the analyst query for each retry attempt when signal is weak."""
    suffixes = [
        ' conflict military violence armed',
        ' war crisis insurgency casualties armed conflict'
    ]
    return query + suffixes[min(attempt, len(suffixes) - 1)]


def _invoke_agent_once(client, session_id: str, query: str) -> dict:
    """
    Single attempt to call the Bedrock Agent and parse its JSON response.
    Returns a result dict on success, a parse_failed result dict when the
    Agent responds with prose instead of a tool result, or raises on
    infrastructure failure (timeout, connection error, etc).
    """
    response = client.invoke_agent(
        agentId     ='OPABTSHSPN',
        agentAliasId='TSTALIASID',
        sessionId   = session_id,
        inputText   = query
    )

    raw = ''
    for event in response['completion']:
        if 'chunk' in event:
            raw += event['chunk']['bytes'].decode('utf-8')

    # The Agent may return prose instead of a tool result —
    # e.g. a clarifying question when the query is ambiguous.
    # That is a different situation from a timeout or a
    # malformed payload, and must not be collapsed into one
    # generic exception.
    json_start = raw.find('{')
    json_end   = raw.rfind('}') + 1

    if json_start == -1 or json_end <= json_start:
        clarify_markers = [
            'could you', 'please clarify', 'can you specify',
            'would you like', 'do you want'
        ]
        lowered = raw.lower()
        if any(m in lowered for m in clarify_markers):
            reason = 'clarification_requested'
        else:
            reason = 'non_json_response'

        return {
            'parse_failed':   True,
            'failure_reason': reason,
            'raw_response':   raw[:500],
        }

    # Extract outermost JSON object from prose-wrapped Bedrock response
    brace_count = 0
    json_end = json_start
    for i in range(json_start, len(raw)):
        if raw[i] == '{':   brace_count += 1
        elif raw[i] == '}': brace_count -= 1
        if brace_count == 0:
            json_end = i + 1
            break

    parsed = json.loads(raw[json_start:json_end])
    raw_confidence = parsed.get('confidence_score', 0.1)
    return {
        'gdelt_events':          parsed.get('events', []),
        'signal_confidence':     max(0.1, raw_confidence),
        'signal_summary':        parsed.get('signal_summary', raw),
        'signal_failure_reason': '',   # explicit clear — prevents a stale
                                       # reason surviving a loop-back success
        'errors':                []
    }


def signal_monitor_node(state: SentinelState) -> dict:
    """
    Agent 1 — Signal Monitor.

    Agentic loop (Change 1): retries up to 3 times with broadened keywords
    when signal_confidence < 0.3 after first attempt. Agent decides when it
    has enough signal to proceed — or returns best available after max tries.

    Reads:  session_id, analyst_query, loop_count
    Writes: gdelt_events, signal_confidence, signal_summary, errors,
            loop_count, signal_failure_reason
    """
    client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')
    query      = state['analyst_query']
    max_attempts = 3
    last_result  = None

    for attempt in range(max_attempts):
        # New session ID per retry so Bedrock Agent doesn't inherit prior
        # low-confidence conversation history
        session_id = (
            state['session_id'] if attempt == 0
            else f"{state['session_id']}-r{attempt}"
        )
        try:
            last_result = _invoke_agent_once(client, session_id, query)

            if last_result.get('parse_failed'):
                reason = last_result.get('failure_reason', 'unknown')
                if reason == 'clarification_requested':
                    summary = (
                        'Agent 1 requested query clarification rather '
                        'than searching. No sources were queried.'
                    )
                else:
                    summary = (
                        f'Agent 1 returned an unparseable response '
                        f'({reason}). No sources were queried.'
                    )
                # Floor confidence is correct here — we genuinely have
                # no signal. But the summary must say WHY.
                return {
                    'signal_confidence':     0.1,
                    'signal_summary':        summary,
                    'signal_failure_reason': reason,
                    'gdelt_events':          [],
                    'errors':                [],
                    'loop_count':            state.get('loop_count', 0) + 1,
                }

            if last_result['signal_confidence'] >= 0.3:
                break  # Agent decides: enough signal
            query = _refine_query(query, attempt)
        except Exception as e:
            last_result = {
                'gdelt_events':          [],
                'signal_confidence':     0.1,
                'signal_summary':        (
                    f'Agent 1 call failed: {str(e)}. No sources were queried.'
                ),
                'signal_failure_reason': 'agent_error',
                'errors':                [f'signal_monitor_node failed (attempt {attempt + 1}): {str(e)}']
            }
            query = _refine_query(query, attempt)

    # Increment loop_count once per node invocation (not per retry attempt)
    last_result['loop_count'] = state.get('loop_count', 0) + 1
    return last_result


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING FUNCTION — after Agent 1
# ─────────────────────────────────────────────────────────────────────────────

def route_after_signal(state: SentinelState) -> str:
    """
    Decides which node runs after Agent 1.
    Returns a string that matches a key in add_conditional_edges() below.

    confidence >= 0.1 → context_historian (Agent 2)
    confidence <  0.1 → clarification (pipeline ends)
    """
    if state['signal_confidence'] >= 0.1:  # Production threshold — 0.1 allows Agent 2 to run
                                             # when GDELT finds nothing (base score) while still
                                             # blocking hard errors. Restored after Day 10 evaluation.
        return 'context_historian'
    else:
        return 'clarification'


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 2 — CONTEXT HISTORIAN NODE
# ─────────────────────────────────────────────────────────────────────────────

def route_after_context(state: SentinelState) -> str:
    """
    Agentic loop (Change 2): if context_confidence < 0.2 and we haven't
    exceeded the loop budget, send Agent 1 back for a broader query.
    Otherwise proceed to Agent 3.
    """
    if state.get('context_confidence', 0.0) < 0.2 and state.get('loop_count', 0) < 2:
        return 'signal_monitor'
    return 'threat_analyst'


def context_historian_node(state: SentinelState) -> dict:
    """
    Agent 2 — Context Historian.
    Runs three retrieval channels then synthesises into one intelligence summary.

    Agentic loop: when invoked as a loop-back from Agent 3 (threat_confidence
    already set in state), increments loop_count to bound the cycle.

    Reads:  analyst_query, signal_summary, loop_count, threat_confidence
    Writes: graph_context, temporal_context, vector_context,
            historian_summary, context_confidence, loop_count
    """
    query          = state['analyst_query']
    signal_summary = state.get('signal_summary', '')

    graph_context    = graph_retrieve(query, signal_summary)
    temporal_context = temporal_retrieve(query, signal_summary)
    vector_context   = vector_retrieve(query, signal_summary)

    synthesis = synthesise(
        query            = query,
        signal_summary   = signal_summary,
        graph_context    = graph_context,
        temporal_context = temporal_context,
        vector_context   = vector_context
    )

    # Increment loop_count when this is a loop-back from Agent 3
    # (threat_confidence already set means Agent 3 has run before this call).
    # Prevents Agent 3 → Agent 2 → Agent 3 infinite cycle.
    loop_increment = 1 if state.get('threat_confidence') else 0

    return {
        'graph_context':      graph_context,
        'temporal_context':   temporal_context,
        'vector_context':     vector_context,
        'historian_summary':  synthesis['historian_summary'],
        'context_confidence': synthesis['context_confidence'],
        'loop_count':         state.get('loop_count', 0) + loop_increment
    }


def threat_analyst_node(state: SentinelState) -> dict:
    """
    Agent 3 — Threat Analyst.

    Reads signal and context outputs from Agents 1 and 2,
    calculates a weighted evidence score in Python, then
    calls Claude to make the categorical threat judgment.

    Why invoke_model() not invoke_agent():
        All data is already in SentinelState — no tools needed.
        This is a pure reasoning step, not an orchestration step.

    Reads:  analyst_query, signal_confidence, context_confidence,
            gdelt_events, historian_summary
    Writes: threat_level, threat_score, threat_rationale,
            key_indicators, threat_confidence
    """
    result = assess_threat(
        analyst_query      = state['analyst_query'],
        signal_confidence  = state['signal_confidence'],
        context_confidence = state['context_confidence'],
        gdelt_events       = state['gdelt_events'],
        historian_summary  = state['historian_summary']
    )
    return result


def red_team_node(state: SentinelState) -> dict:
    """
    Agent 4 — Red Team.

    Only runs when threat_score >= 0.7.
    Adversarially challenges Agent 3's assessment.
    Can only reduce the threat score, never increase it.

    Reads:  analyst_query, signal_summary, historian_summary,
            threat_level, threat_score, threat_rationale,
            key_indicators, threat_confidence, gdelt_events
    Writes: red_team_assessment, counter_evidence,
            revised_threat_score, revised_confidence
    """
    result = challenge_assessment(
        analyst_query     = state['analyst_query'],
        signal_summary    = state['signal_summary'],
        historian_summary = state['historian_summary'],
        threat_level      = state['threat_level'],
        threat_score      = state['threat_score'],
        threat_rationale  = state['threat_rationale'],
        key_indicators    = state['key_indicators'],
        threat_confidence = state['threat_confidence'],
        gdelt_events      = state['gdelt_events']
    )
    return result


def route_after_threat(state: SentinelState) -> str:
    """
    Agentic loop (Change 3): if threat_confidence is LOW and loop budget
    allows, loop back to Agent 2 for more context before finalising.
    Otherwise route to red_team if threat_score >= 0.7 OR threat_level
    is HIGH — either signal alone is sufficient to trigger challenge.
    """
    if state.get('threat_confidence') == 'LOW' and state.get('loop_count', 0) < 2:
        return 'context_historian'

    if should_red_team(state):
        return 'red_team'
    return 'end'


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline():
    """
    Assembles and compiles the LangGraph StateGraph.

    Agentic loop shape:
        signal_monitor ──► context_historian ──► threat_analyst ──► red_team ──► END
             ▲                    │                    │
             │   (conf < 0.2)     │   (conf LOW)       └──► END
             └────────────────────┘◄───────────────────┘
    """
    graph = StateGraph(SentinelState)

    # Register nodes
    graph.add_node('signal_monitor',    signal_monitor_node)
    graph.add_node('context_historian', context_historian_node)
    graph.add_node('threat_analyst',    threat_analyst_node)
    graph.add_node('red_team',          red_team_node)

    # Entry point
    graph.set_entry_point('signal_monitor')

    # Conditional routing after Agent 1
    graph.add_conditional_edges(
        'signal_monitor',
        route_after_signal,
        {
            'context_historian': 'context_historian',
            'clarification':      END
        }
    )

    # Change 2: context sufficiency check — may loop back to Agent 1
    graph.add_conditional_edges(
        'context_historian',
        route_after_context,
        {
            'threat_analyst': 'threat_analyst',
            'signal_monitor': 'signal_monitor'
        }
    )

    # Change 3: end-turn decision — may loop back to Agent 2
    graph.add_conditional_edges(
        'threat_analyst',
        route_after_threat,
        {
            'red_team':        'red_team',
            'context_historian': 'context_historian',
            'end':              END
        }
    )
    graph.add_edge('red_team', END)

    return graph.compile()


# ─────────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL PIPELINE INSTANCE
# ─────────────────────────────────────────────────────────────────────────────

# Runs once on import. Reused by all subsequent imports.
# Usage: from orchestration.pipeline import pipeline
pipeline = build_pipeline()


# ─────────────────────────────────────────────────────────────────────────────
# RUN WRAPPER — single entry point for a full pipeline run
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(analyst_query: str, session_id: str = None,
                 condition: str = 'production') -> dict:
    """
    Single entry point for a full pipeline run.
    Owns all run-completion concerns so that every
    caller gets identical behaviour: Flash Report,
    episode logging, and latency measurement.
    Callers should use this, not pipeline.invoke().

    condition: retrieval-condition tag written to the episode log.
    Defaults to 'production'; the Day 17 harness overrides it to
    label runs by experimental condition.
    """
    import time, uuid
    from agents.episodic.episode_log import save_episode

    session_id = session_id or str(uuid.uuid4())
    initial_state = {
        'analyst_query':         analyst_query,
        'session_id':            session_id,
        'loop_count':            0,
        'signal_failure_reason': '',
    }

    start   = time.time()
    result  = pipeline.invoke(initial_state)
    latency = time.time() - start

    # Run-completion concerns live here, not in callers.
    flash = generate_flash_report(result)

    # Callers get the complete run outcome. Anything that
    # needed the flash report should not have to regenerate it.
    result['flash_report']  = flash
    result['report_id']     = flash['report_id']
    result['total_latency'] = latency

    print(f'[pipeline] Flash Report: {flash["report_id"]}')
    print(f'[pipeline] Latency: {latency:.1f}s')

    # Log the episode. Write-only: this is the system's own
    # run history, used as the Day 17 calibration dataset.
    # save_episode never raises, so it cannot break a run.
    # result['report_id'] was set by the flash-report block above.
    save_episode(result, latency, condition=condition)

    return result