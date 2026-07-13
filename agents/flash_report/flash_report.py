# ─────────────────────────────────────────────────────────────────────────────
# agents/flash_report/flash_report.py
#
# Terminal output of the 4-agent pipeline.
# Called after pipeline.invoke() completes.
# Saves structured JSON to S3 and locally.
# Never crashes the pipeline — all errors caught.
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
import uuid
import boto3
from datetime import datetime, timezone

S3_BUCKET = 'sentinel-osint-knowledge-base'
S3_PREFIX = 'reports'
LOCAL_DIR = 'flash_reports'


def build_data_gaps(state: dict) -> list:
    """
    Derive explicit data gap strings from channel
    activation counts. Called by generate_flash_report.
    Returns a list of plain English gap descriptions.
    """
    gaps = []

    # Agent 1 gaps
    if len(state.get('gdelt_events', [])) == 0:
        gaps.append(
            'No live GDELT events found — '
            'signal based on base confidence only'
        )

    # Agent 2 channel gaps
    if len(state.get('vector_context', [])) == 0:
        gaps.append(
            'No KB document chunks retrieved — '
            'documentary evidence absent'
        )

    graph = state.get('graph_context', [])
    if len(graph) < 3:
        gaps.append(
            'Knowledge graph sparse for this region — '
            'structural relationships limited'
        )

    temporal = state.get('temporal_context', [])
    if len(temporal) == 0 or (
        len(temporal) == 1 and
        temporal[0].get('id') == 'COVERAGE_GAP'
    ):
        gaps.append(
            'No temporal event chain found — '
            'causal history unavailable for this region'
        )

    # Red team gap — check whether it actually ran,
    # do not re-derive the trigger condition here.
    if not state.get('red_team_assessment', ''):
        gaps.append(
            'Red team not triggered — assessment was not '
            'flagged as high-threat and did not undergo '
            'adversarial review'
        )

    return gaps


def build_executive_summary(state: dict) -> str:
    """
    Build a 2-sentence executive summary from
    threat_level and threat_rationale.
    First sentence: the bottom line.
    Second sentence: the key caveat.
    """
    threat_level  = state.get('threat_level', 'UNKNOWN')
    threat_conf   = state.get('threat_confidence', 'LOW')
    rationale     = state.get('threat_rationale', '')
    query         = state.get('analyst_query', '')

    # First sentence — the bottom line
    sentence1 = (
        f'Assessment of "{query}" yields a '
        f'{threat_level} threat designation '
        f'with {threat_conf} analytical confidence.'
    )

    # Second sentence — key caveat from rationale
    # Take first 200 chars of rationale as the caveat
    if rationale:
        caveat = rationale[:200].rstrip()
        if len(rationale) > 200:
            caveat += '...'
        sentence2 = caveat
    else:
        sentence2 = (
            'Insufficient evidence to support '
            'a higher confidence assessment.'
        )

    return f'{sentence1} {sentence2}'


def build_narrative_prompt(state: dict) -> str:
    """
    Assemble the prompt for the narrative Flash Report.
    Feeds Claude the full pipeline output and asks for
    an analyst-grade prose product.
    """
    return f"""You are a senior geopolitical intelligence
analyst writing a Flash Report for a decision-maker who has
not seen any of the underlying data.

Write a structured prose intelligence report. Do not use
bullet points. Do not restate the raw numbers as numbers --
translate them into analytical language.

ANALYST QUERY:
{state.get('analyst_query', '')}

LIVE SIGNAL (Agent 1):
confidence {state.get('signal_confidence', 0.0)}
{state.get('signal_summary', '')}

HISTORICAL CONTEXT (Agent 2):
confidence {state.get('context_confidence', 0.0)}
channels -- graph: {len(state.get('graph_context', []))},
temporal: {len(state.get('temporal_context', []))},
vector: {len(state.get('vector_context', []))}
{state.get('historian_summary', '')}

THREAT ASSESSMENT (Agent 3):
level {state.get('threat_level', 'UNKNOWN')},
score {state.get('threat_score', 0.0)},
confidence {state.get('threat_confidence', 'LOW')}
{state.get('threat_rationale', '')}
key indicators: {state.get('key_indicators', [])}

RED TEAM CHALLENGE (Agent 4):
{state.get('red_team_assessment', 'Not triggered -- assessment was not flagged as high-threat.')}
counter-evidence: {state.get('counter_evidence', [])}
revised score: {state.get('revised_threat_score', 'n/a')}

Write the report with these sections, as flowing prose under
each heading:

## Bottom Line
Two to three sentences. What is the assessment, and how much
should the reader trust it? State the threat level and be
explicit about confidence. If confidence is low, say so
plainly in the first paragraph -- do not bury it.

## Current Signal
What the live data actually shows. Be specific about named
actors, locations and dates. If the live signal did not
directly address the query, say so explicitly rather than
implying coverage that does not exist.

## Historical Context
How the situation developed. Draw the causal chain. Name the
actors and their relationships.

## Assessment
The analytical judgment and the reasoning behind it. Explain
WHY the evidence supports this threat level.

## Challenge and Counter-Evidence
If the red team ran, summarise its critique honestly and
explain how it changes the assessment. If it did not run,
state that the assessment has not undergone adversarial
review.

## Intelligence Gaps
What is missing, and what the reader should NOT conclude
from this report. Be concrete about which knowledge types
were absent -- structural relationships, causal history, or
documentary evidence.

Write in measured analytical register. Hedge where the
evidence is thin. Never fabricate specifics not present in
the data above. If a section has no supporting data, say so
rather than padding it."""


def generate_narrative(state: dict) -> str:
    """
    Single invoke_model() call producing the prose report.
    Returns the narrative string, or a fallback message
    on failure -- must never crash the pipeline.
    """
    try:
        client = boto3.client(
            'bedrock-runtime', region_name='us-east-1'
        )
        body = {
            'anthropic_version': 'bedrock-2023-05-31',
            'max_tokens': 2000,
            'messages': [{
                'role': 'user',
                'content': build_narrative_prompt(state)
            }]
        }
        resp = client.invoke_model(
            modelId='us.anthropic.claude-sonnet-4-6',
            body=json.dumps(body)
        )
        parsed = json.loads(resp['body'].read())
        return parsed['content'][0]['text']
    except Exception as e:
        print(f'[flash_report] Narrative failed: {str(e)}')
        return (
            'Narrative generation unavailable. '
            'See structured fields for the assessment.'
        )


def generate_flash_report(state: dict) -> dict:
    """
    Generate the terminal Flash Report for a
    completed 4-agent pipeline run.

    Reads all fields from SentinelState.
    Builds a structured report dict.
    Saves to S3 and locally.
    Returns the report dict regardless of save status.

    Args:
        state: Final SentinelState dict after
               pipeline.invoke() completes

    Returns:
        report dict with all fields populated
    """

    # ── Build the report ──────────────────────────
    report_id = str(uuid.uuid4())
    now       = datetime.now(timezone.utc).isoformat()

    report = {

        # ── Identity ──────────────────────────────
        'report_id':    report_id,
        'generated_at': now,
        'session_id':   state.get('session_id', ''),
        'analyst_query':state.get('analyst_query', ''),

        # ── Executive summary ─────────────────────
        'executive_summary': build_executive_summary(state),

        # ── Agent 1 outputs ───────────────────────
        'signal_confidence': state.get('signal_confidence', 0.0),
        'signal_summary':    state.get('signal_summary', ''),
        'gdelt_event_count': len(state.get('gdelt_events', [])),

        # ── Agent 2 outputs ───────────────────────
        'context_confidence': state.get('context_confidence', 0.0),
        'historical_context': state.get('historian_summary', ''),
        'channel_activation': {
            'graph_count':    len(state.get('graph_context', [])),
            'temporal_count': len(state.get('temporal_context', [])),
            'vector_count':   len(state.get('vector_context', [])),
            'gdelt_events':   len(state.get('gdelt_events', [])),
        },

        # ── Agent 3 outputs ───────────────────────
        'threat_level':      state.get('threat_level', 'UNKNOWN'),
        'threat_score':      state.get('threat_score', 0.0),
        'threat_confidence': state.get('threat_confidence', 'LOW'),
        'threat_rationale':  state.get('threat_rationale', ''),
        'key_indicators':    state.get('key_indicators', []),

        # ── Agent 4 outputs ───────────────────────
        'revised_threat_score': state.get(
            'revised_threat_score',
            state.get('threat_score', 0.0)
        ),
        'revised_confidence':   state.get(
            'revised_confidence',
            state.get('threat_confidence', 'LOW')
        ),
        'red_team_assessment':  state.get(
            'red_team_assessment', ''
        ),
        'counter_evidence':     state.get(
            'counter_evidence', []
        ),

        # ── Confidence scores ─────────────────────
        'confidence_scores': {
            'signal':   state.get('signal_confidence', 0.0),
            'context':  state.get('context_confidence', 0.0),
            'threat':   state.get('threat_confidence', 'LOW'),
            'revised':  state.get('revised_confidence', 'LOW'),
        },

        # ── Data gaps ─────────────────────────────
        'data_gaps': build_data_gaps(state),

        # ── Pipeline metadata ─────────────────────
        'loop_count': state.get('loop_count', 0),
    }

    # Prose intelligence product for human readers.
    # The structured fields above remain the machine-
    # readable source of truth.
    report['narrative_report'] = generate_narrative(state)

    # ── Save locally ──────────────────────────────
    try:
        os.makedirs(LOCAL_DIR, exist_ok=True)
        local_path = os.path.join(
            LOCAL_DIR, f'{report_id}.json'
        )
        with open(local_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f'[flash_report] Saved locally: {local_path}')
    except Exception as e:
        print(f'[flash_report] Local save failed: {str(e)}')

    # ── Save markdown ─────────────────────────────
    try:
        md_path = os.path.join(
            LOCAL_DIR, f'{report_id}.md'
        )
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(f"# Flash Report\n\n")
            f.write(f"**Report ID:** {report_id}  \n")
            f.write(
                f"**Generated:** {report['generated_at']}  \n"
            )
            f.write(
                f"**Query:** "
                f"{report['analyst_query']}\n\n---\n\n"
            )
            f.write(report['narrative_report'])
            f.write("\n\n---\n\n## Data Gaps\n\n")
            for gap in report['data_gaps']:
                f.write(f"- {gap}\n")
        print(f'[flash_report] Markdown: {md_path}')
    except Exception as e:
        print(f'[flash_report] Markdown save failed: {str(e)}')

    # ── Save to S3 ────────────────────────────────
    try:
        s3  = boto3.client('s3', region_name='us-east-1')
        key = f'{S3_PREFIX}/{report_id}.json'
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = key,
            Body        = json.dumps(report, indent=2).encode(),
            ContentType = 'application/json'
        )
        print(f'[flash_report] Saved to S3: s3://{S3_BUCKET}/{key}')
    except Exception as e:
        print(f'[flash_report] S3 save failed (non-fatal): {str(e)}')

    return report
