"""
Branch-coverage test for Agent 1 failure handling.

Confirms that on EVERY return path of signal_monitor_node the two
fields that describe "why is there no signal" stay consistent:
    signal_failure_reason  (machine-readable reason code)
    signal_summary         (human-readable explanation)

Also confirms _invoke_agent_once classifies a prose response as
clarification_requested vs non_json_response correctly — the
non_json_response path cannot be triggered reliably against live
Bedrock, so it is proven here.

Pure unit test — no AWS calls. Run: python test_signal_branches.py
"""
import sys
from unittest import mock

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from orchestration import pipeline
from orchestration.pipeline import _invoke_agent_once, signal_monitor_node


def _fake_client(raw_text: str):
    """A stand-in bedrock-agent-runtime client whose invoke_agent streams raw_text."""
    client = mock.MagicMock()
    client.invoke_agent.return_value = {
        'completion': [
            {'chunk': {'bytes': raw_text.encode('utf-8')}}
        ]
    }
    return client


failures = []

def check(label, cond, detail=''):
    status = 'PASS' if cond else 'FAIL'
    print(f'  [{status}] {label}' + (f' — {detail}' if detail and not cond else ''))
    if not cond:
        failures.append(label)


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — _invoke_agent_once classification (real parse logic, fake transport)
# ─────────────────────────────────────────────────────────────────────────────
print('\nPART 1 — _invoke_agent_once classification')

# 1a. Valid JSON → success, reason cleared to ''
valid = '{"events": [], "confidence_score": 0.7, "signal_summary": "ok"}'
r = _invoke_agent_once(_fake_client(valid), 'sid', 'q')
check('valid JSON → not parse_failed', not r.get('parse_failed'))
check("valid JSON → signal_failure_reason == ''", r.get('signal_failure_reason') == '',
      repr(r.get('signal_failure_reason')))

# 1b. Prose with a clarify marker → clarification_requested
clarify = "I'm not sure what you mean. Could you please clarify the region?"
r = _invoke_agent_once(_fake_client(clarify), 'sid', 'q')
check('clarify prose → parse_failed', r.get('parse_failed') is True)
check("clarify prose → reason == 'clarification_requested'",
      r.get('failure_reason') == 'clarification_requested', repr(r.get('failure_reason')))

# 1c. Prose WITHOUT any clarify marker and no JSON → non_json_response
prose = "The situation is complex and evolving across the region this year."
r = _invoke_agent_once(_fake_client(prose), 'sid', 'q')
check('plain prose → parse_failed', r.get('parse_failed') is True)
check("plain prose → reason == 'non_json_response'",
      r.get('failure_reason') == 'non_json_response', repr(r.get('failure_reason')))


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — signal_monitor_node: reason ↔ summary agreement on all 4 branches
# ─────────────────────────────────────────────────────────────────────────────
print('\nPART 2 — signal_monitor_node branch agreement')

base_state = {'analyst_query': 'q', 'session_id': 'sid', 'loop_count': 0}

def run_node(side_effect):
    """Patch _invoke_agent_once and boto3.client, then run the node."""
    with mock.patch.object(pipeline, '_invoke_agent_once', side_effect=side_effect), \
         mock.patch.object(pipeline.boto3, 'client', return_value=mock.MagicMock()):
        return signal_monitor_node(dict(base_state))

# 2a. SUCCESS path — reason must be '' and summary must be the real one
def _ok(client, sid, q):
    return {'gdelt_events': [], 'signal_confidence': 0.7,
            'signal_summary': 'A real signal summary.',
            'signal_failure_reason': '', 'errors': []}
out = run_node(_ok)
check("success → signal_failure_reason == ''", out.get('signal_failure_reason') == '',
      repr(out.get('signal_failure_reason')))
check('success → summary is the real summary',
      out.get('signal_summary') == 'A real signal summary.', repr(out.get('signal_summary')))
check('success → errors present ([])', out.get('errors') == [])

# 2b. clarification_requested — summary must mention clarification
def _clar(client, sid, q):
    return {'parse_failed': True, 'failure_reason': 'clarification_requested', 'raw_response': '...'}
out = run_node(_clar)
check("clarification → reason code", out.get('signal_failure_reason') == 'clarification_requested')
check('clarification → summary agrees ("clarification" in text)',
      'clarification' in out.get('signal_summary', '').lower(), repr(out.get('signal_summary')))
check('clarification → errors present ([])', out.get('errors') == [])

# 2c. non_json_response — summary must name the reason
def _nonjson(client, sid, q):
    return {'parse_failed': True, 'failure_reason': 'non_json_response', 'raw_response': '...'}
out = run_node(_nonjson)
check("non_json → reason code", out.get('signal_failure_reason') == 'non_json_response')
check('non_json → summary agrees ("unparseable"/reason in text)',
      'non_json_response' in out.get('signal_summary', ''), repr(out.get('signal_summary')))
check('non_json → errors present ([])', out.get('errors') == [])

# 2d. agent_error — _invoke_agent_once raises on all 3 attempts
def _boom(client, sid, q):
    raise Exception('Read timed out.')
out = run_node(_boom)
check("agent_error → reason code", out.get('signal_failure_reason') == 'agent_error')
check('agent_error → summary starts "Agent 1 call failed:"',
      out.get('signal_summary', '').startswith('Agent 1 call failed:'), repr(out.get('signal_summary')))
check('agent_error → error message captured',
      any('Read timed out' in e for e in out.get('errors', [])), repr(out.get('errors')))


# ─────────────────────────────────────────────────────────────────────────────
print('\n' + '=' * 60)
if failures:
    print(f'FAILED — {len(failures)} check(s): {failures}')
    sys.exit(1)
print('ALL BRANCHES PASSED — reason code and summary agree on every path')
print('=' * 60)
