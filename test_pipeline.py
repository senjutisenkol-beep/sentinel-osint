# ─────────────────────────────────────────────────────────────────────────────
# test_pipeline.py  (project root)
#
# End-to-end runner for the Sentinel-OSINT pipeline.
# Run from the project root with: python test_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

import sys
import uuid
import warnings

sys.stdout.reconfigure(encoding='utf-8')

# Suppress the Pydantic V1 / Python 3.14 compatibility warning —
# cosmetic only, does not affect pipeline behaviour
warnings.filterwarnings('ignore', category=UserWarning)

from orchestration.pipeline import run_pipeline, should_red_team

# ── Run pipeline ──────────────────────────────────────────────────────────────
analyst_query = 'Israel Gaza military offensive 2026'
session_id    = str(uuid.uuid4())

print('\n' + '='*60)
print('SENTINEL-OSINT — END-TO-END TEST')
print('='*60)
print(f"Query:   {analyst_query}")
print(f"Session: {session_id}")
print('='*60 + '\n')

print('► Running pipeline...\n')
result = run_pipeline(analyst_query, session_id=session_id)

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
print(f"\nSignal summary:")
print(result['signal_summary'])

# ── Agent 1 results ───────────────────────────────────────────────────────────
print('─'*60)
print('AGENT 1 — SIGNAL MONITOR')
print('─'*60)
print(f"Signal confidence:  {result['signal_confidence']}")
print(f"GDELT events found: {len(result['gdelt_events'])}")
print(f"Errors:             {result['errors']}")
print(f"\nSignal summary:")
print(result['signal_summary'])

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

print('\n' + '─'*60)
print('AGENT 3 — THREAT ANALYST')
print('─'*60)
print(f'Threat level:      {result["threat_level"]}')
print(f'Threat score:      {result["threat_score"]}')
print(f'Threat confidence: {result["threat_confidence"]}')
print(f'\nKey indicators:')
for ind in result['key_indicators']:
    print(f'  • {ind}')
print(f'\nThreat rationale:')
print(result['threat_rationale'])

print()
if should_red_team(result):
    print('─'*60)
    print('AGENT 4 — RED TEAM')
    print('─'*60)
    print(f'Revised threat score: {result["revised_threat_score"]}')
    print(f'Revised confidence:   {result["revised_confidence"]}')
    print(f'\nCounter evidence:')
    for c in result['counter_evidence']:
        print(f'  • {c}')
    print(f'\nRed team assessment:')
    print(result['red_team_assessment'])
else:
    print('─'*60)
    print('AGENT 4 — RED TEAM')
    print('─'*60)
    print(
        f'Skipped — threat_score {result["threat_score"]:.3f} < 0.7 '
        f'and threat_level {result["threat_level"]!r} != HIGH'
    )

print()
print('='*60)
print(f'Flash Report ID: {result["report_id"]}')
print(f'Total latency:   {result["total_latency"]:.1f}s')
print('Data gaps:')
for gap in result['flash_report'].get('data_gaps', []):
    print(f'  • {gap}')
print('='*60)

print('\n' + '='*60)
print('FLASH REPORT — NARRATIVE (Claude prose)')
print('='*60)
print(result['flash_report']['narrative_report'])
print('='*60)

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