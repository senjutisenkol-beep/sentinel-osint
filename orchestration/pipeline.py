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


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 1 — SIGNAL MONITOR NODE
# ─────────────────────────────────────────────────────────────────────────────

def signal_monitor_node(state: SentinelState) -> dict:
    """
    Agent 1 — Signal Monitor.
    Invokes the Bedrock Agent (OPABTSHSPN) which calls the
    sentinel-signal-monitor-gdelt Lambda to fetch live GDELT events.

    Reads:  session_id, analyst_query
    Writes: gdelt_events, signal_confidence, signal_summary, errors
    """
    client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')

    try:
        response = client.invoke_agent(
            agentId='OPABTSHSPN',
            agentAliasId='TSTALIASID',
            sessionId=state['session_id'],
            inputText=state['analyst_query']
        )

        # Bedrock streams response in chunks — accumulate into one string
        raw = ''
        for events in response['completion']:
            if 'chunk' in events:
                raw += events['chunk']['bytes'].decode('utf-8')

        # Agent 1 embeds JSON inside prose text — extract it by tracking braces
        json_start = raw.find('{')
        brace_count = 0
        json_end = json_start

        for i in range(json_start, len(raw)):
            if raw[i] == '{':
                brace_count += 1
            elif raw[i] == '}':
                brace_count -= 1
            if brace_count == 0:
                json_end = i + 1
                break

        json_block = raw[json_start:json_end]
        parsed = json.loads(json_block)

        # Return only changed fields — LangGraph merges into full state
        return {
            'gdelt_events':      parsed.get('events', []),
            'signal_confidence': parsed.get('confidence_score', 0.1),
            'signal_summary':    raw,
            'errors':            []
        }

    except Exception as e:
        # Safe fallback — pipeline continues, error logged in state
        return {
            'gdelt_events':      [],
            'signal_confidence': 0.1,
            'signal_summary':    '',
            'errors':            [f'signal_monitor_node failed: {str(e)}']
        }


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING FUNCTION — after Agent 1
# ─────────────────────────────────────────────────────────────────────────────

def route_after_signal(state: SentinelState) -> str:
    """
    Decides which node runs after Agent 1.
    Returns a string that matches a key in add_conditional_edges() below.

    confidence >= 0.4 → context_historian (Agent 2)
    confidence <  0.4 → clarification (pipeline ends)
    """
    if state['signal_confidence'] >= 0.05:
        return 'context_historian'
    else:
        return 'clarification'


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 2 — CONTEXT HISTORIAN NODE
# ─────────────────────────────────────────────────────────────────────────────

def context_historian_node(state: SentinelState) -> dict:
    """
    Agent 2 — Context Historian.
    Runs three retrieval channels then synthesises into one intelligence summary.

    Reads:  analyst_query, signal_summary
    Writes: graph_context, temporal_context, vector_context,
            historian_summary, context_confidence
    """
    query          = state['analyst_query']
    signal_summary = state.get('signal_summary', '')

    # Channel 1 — graph traversal: entity relationships from knowledge graph
    graph_context = graph_retrieve(query, signal_summary)

    # Channel 2 — temporal: chronological Event nodes from knowledge graph
    temporal_context = temporal_retrieve(query, signal_summary)

    # Channel 3 — vector: semantically relevant chunks from Bedrock KB
    vector_context = vector_retrieve(query, signal_summary)

    # Synthesise all three channels into one summary via direct Claude call
    synthesis = synthesise(
        query            = query,
        signal_summary   = signal_summary,
        graph_context    = graph_context,
        temporal_context = temporal_context,
        vector_context   = vector_context
    )

    return {
        'graph_context':      graph_context,
        'temporal_context':   temporal_context,
        'vector_context':     vector_context,
        'historian_summary':  synthesis['historian_summary'],
        'context_confidence': synthesis['context_confidence']
    }


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline():
    """
    Assembles and compiles the LangGraph StateGraph.

    Current shape:
                        ┌─► context_historian ─► END
        signal_monitor ─┤
                        └─► clarification ─────► END
    """
    graph = StateGraph(SentinelState)

    # Register nodes
    graph.add_node('signal_monitor',    signal_monitor_node)
    graph.add_node('context_historian', context_historian_node)

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

    # After Agent 2 → END for now. Week 3: replace with 'threat_analyst'
    graph.add_edge('context_historian', END)

    return graph.compile()


# ─────────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL PIPELINE INSTANCE
# ─────────────────────────────────────────────────────────────────────────────

# Runs once on import. Reused by all subsequent imports.
# Usage: from orchestration.pipeline import pipeline
pipeline = build_pipeline()