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


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 1 — SIGNAL MONITOR NODE
# ─────────────────────────────────────────────────────────────────────────────

def signal_monitor_node(state: SentinelState) -> dict:
    """
    Agent 1 — Signal Monitor.

    Architecture (production-grade):
        Analyst query
            → invoke_agent() — Bedrock Agent OPABTSHSPN
                → Claude transforms query into smart keywords
                → Calls Lambda tool (sentinel-signal-monitor-gdelt)
                    → Lambda calls MCP server query_all_sources()
                        → GDELT + NewsAPI + ReliefWeb merged feed
                → Returns structured JSON with confidence score

    Why Bedrock Agent instead of direct Python call:
        Claude inside the Bedrock Agent reads the analyst's natural
        language query and intelligently decides what keywords to
        search for — understanding context, synonyms, and domain
        terminology. A simple string split cannot do this.
        The MCP server handles multi-source data merging.
        Bedrock handles intelligent query transformation.
        Both layers are needed.

    Reads from state:
        session_id:    unique ID for this pipeline run
        analyst_query: raw natural language question from analyst

    Writes to state:
        gdelt_events:      merged event list from all three sources
        signal_confidence: corroboration-based score (not event count)
        signal_summary:    prose summary for Agent 2 query enrichment
        errors:            list of error strings if something failed
    """

    # Create the Bedrock Agent Runtime client.
    # bedrock-agent-runtime is used for invoking Bedrock Agents and
    # querying Knowledge Bases. Different from bedrock-runtime which
    # is for direct model calls (used in synthesiser.py).
    client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')

    try:
        # invoke_agent sends the analyst's natural language query to
        # the Bedrock Agent. Claude inside the agent reads the query,
        # reasons about what keywords to search for, and calls the
        # Lambda tool via the Action Group mechanism.
        #
        # agentId:      OPABTSHSPN — our Signal Monitor agent in Bedrock
        # agentAliasId: TSTALIASID — test alias pointing to latest draft
        # sessionId:    ties this call to the current pipeline run session
        # inputText:    the raw analyst query — Claude transforms this
        response = client.invoke_agent(
            agentId     ='OPABTSHSPN',
            agentAliasId='TSTALIASID',
            sessionId   = state['session_id'],
            inputText   = state['analyst_query']
        )

        # Bedrock streams the response back in chunks rather than one
        # payload. We loop through every streaming event and accumulate
        # the text into a single string called raw.
        raw = ''
        for events in response['completion']:
            if 'chunk' in events:
                # Each chunk arrives as raw bytes — decode to UTF-8 string
                raw += events['chunk']['bytes'].decode('utf-8')

        # The Bedrock Agent embeds a JSON object inside its text response.
        # We cannot use json.loads(raw) directly because there may be
        # surrounding prose text from Claude's reasoning.
        # Instead we find the JSON block by tracking opening and closing
        # braces — when brace_count returns to 0 we have found the full
        # outermost JSON object.
        json_start = raw.find('{')   # Position of first opening brace
        brace_count = 0
        json_end = json_start

        for i in range(json_start, len(raw)):
            if raw[i] == '{':
                brace_count += 1   # Going deeper into nested JSON
            elif raw[i] == '}':
                brace_count -= 1   # Coming back out
            if brace_count == 0:
                # brace_count back to zero means we closed the outermost
                # JSON object — this is where the block ends
                json_end = i + 1
                break

        # Slice out just the JSON block and parse it into a Python dict
        json_block = raw[json_start:json_end]
        parsed = json.loads(json_block)

        # The Lambda now returns a richer response from the MCP server:
        # events:           merged list from GDELT + NewsAPI + ReliefWeb
        # confidence_score: corroboration-based (0.7 = two sources agree)
        # signal_summary:   pre-built prose with source counts + top events
        #
        # signal_summary is passed to Agent 2 which uses the first 300
        # characters for query expansion in vector retrieval — the richer
        # this summary is, the better the KB chunks Agent 2 will find.
        return {
            'gdelt_events':      parsed.get('events', []),
            'signal_confidence': parsed.get('confidence_score', 0.1),
            'signal_summary':    parsed.get('signal_summary', raw),
            'errors':            []
        }

    except Exception as e:
        # If anything fails — network error, Bedrock throttle, malformed
        # JSON — return safe fallback values so the pipeline can still
        # route correctly. The error is logged in state errors list.
        # signal_confidence 0.1 is the base score — above the 0.1
        # routing threshold so Agent 2 still runs even on Agent 1 failure.
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