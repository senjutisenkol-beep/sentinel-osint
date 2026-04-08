# ─────────────────────────────────────────────────────────────────────────────
# test_pipeline.py  (project root)
#
# End-to-end runner for the Sentinel-OSINT pipeline.
# Run from the project root with: python test_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

import uuid
import warnings
from datetime import datetime, timezone

# Suppress the Pydantic V1 / Python 3.14 compatibility warning —
# cosmetic only, does not affect pipeline behaviour
warnings.filterwarnings('ignore', category=UserWarning)

from orchestration.pipeline import pipeline

# ── Initial state — all SentinelState fields must be present ─────────────────
initial_state = {
    # Pipeline-level
    'session_id':    str(uuid.uuid4()),
    'analyst_query': 'Find conflict events in Mali regarding JNIM insurgency',
    'retrieved_at':  datetime.now(timezone.utc).isoformat(),

    # Agent 1 defaults — overwritten by signal_monitor_node
    'gdelt_events':      [],
    'signal_confidence': 0.0,
    'signal_summary':    '',

    # Pipeline control
    'abort':        False,
    'abort_reason': '',

    # Error tracking
    'errors': [],

    # Agent 2 defaults — overwritten by context_historian_node
    'graph_context':      [],
    'temporal_context':   [],
    'vector_context':     [],
    'historian_summary':  '',
    'context_confidence': 0.0,
}

# ── Run pipeline ──────────────────────────────────────────────────────────────
print('\n' + '='*60)
print('SENTINEL-OSINT — END-TO-END TEST')
print('='*60)
print(f"Query:   {initial_state['analyst_query']}")
print(f"Session: {initial_state['session_id']}")
print('='*60 + '\n')

print('► Running pipeline...\n')
result = pipeline.invoke(initial_state)

# ── Routing decision ──────────────────────────────────────────────────────────
print(f"Routing decision: confidence={result['signal_confidence']} → ", end='')
if result['signal_confidence'] >= 0.05:
    print('context_historian ✅')
else:
    print('clarification ⚠️  (Agent 2 skipped)')

# ── Agent 1 results ───────────────────────────────────────────────────────────
print('\n' + '─'*60)
print('AGENT 1 — SIGNAL MONITOR')
print('─'*60)
print(f"Signal confidence:  {result['signal_confidence']}")
print(f"GDELT events found: {len(result['gdelt_events'])}")
print(f"Errors:             {result['errors']}")
print(f"\nSignal summary (first 400 chars):")
print(result['signal_summary'][:400])

# ── Agent 1 results ───────────────────────────────────────────────────────────
print('─'*60)
print('AGENT 1 — SIGNAL MONITOR')
print('─'*60)
print(f"Signal confidence:  {result['signal_confidence']}")
print(f"GDELT events found: {len(result['gdelt_events'])}")
print(f"Errors:             {result['errors']}")
print(f"\nSignal summary (first 400 chars):")
print(result['signal_summary'][:400])

# ── Agent 2 results ───────────────────────────────────────────────────────────
print('\n' + '─'*60)
print('AGENT 2 — CONTEXT HISTORIAN')
print('─'*60)

print(f"\nChannel 1 — Graph ({len(result['graph_context'])} relationships):")
for rel in result['graph_context'][:5]:
    print(f"  {rel}")

print(f"\nChannel 2 — Temporal ({len(result['temporal_context'])} events):")
for event in result['temporal_context'][:3]:
    print(f"  [{event.get('date','?')}] {event.get('id','?')}")

print(f"\nChannel 3 — Vector ({len(result['vector_context'])} chunks):")
for chunk in result['vector_context']:
    print(f"  score={chunk.get('score')} | {chunk.get('source','?')[:60]}")

print(f"\nContext confidence: {result['context_confidence']}")
print(f"\nHistorian summary:")
print(result['historian_summary'])

# ── Pass/fail ─────────────────────────────────────────────────────────────────
print('\n' + '='*60)
agent1_ok = result['signal_confidence'] > 0.0
agent2_ok = len(result['temporal_context']) > 0 and result['historian_summary'] != ''

if agent1_ok and agent2_ok:
    print('✅ PASSED — both agents produced output')
elif agent1_ok:
    print('⚠️  PARTIAL — Agent 1 OK, Agent 2 produced no output')
else:
    print('❌ FAILED — Agent 1 produced no output')
print('='*60 + '\n')