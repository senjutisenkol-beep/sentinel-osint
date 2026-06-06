# ─────────────────────────────────────────────────────────────────────────────
# agents/red_team/red_team.py
#
# Agent 4 — Red Team
#
# Purpose:
#   Adversarially challenge Agent 3's threat assessment when
#   threat_score >= 0.7. High-stakes assessments warrant scrutiny
#   before reaching the Flash Report.
#
# Three challenges applied:
#   1. Source critique — are sources reliable and independent?
#   2. Alternative explanations — could evidence support a
#      different conclusion or actor?
#   3. Confidence inflation — is Agent 3 overconfident given
#      actual data quality?
#
# Architecture:
#   invoke_model() directly — no tools needed, all data in state
#   Red team can only temper score, never inflate it
#
# Routing:
#   Only runs when threat_score >= 0.7 (CHOICE in pipeline)
#   Below 0.7 pipeline routes directly to END
# ─────────────────────────────────────────────────────────────────────────────

import json
import boto3
from typing import List


def build_red_team_prompt(
    analyst_query:    str,
    signal_summary:   str,
    historian_summary:str,
    threat_level:     str,
    threat_score:     float,
    threat_rationale: str,
    key_indicators:   List[str],
    threat_confidence:str,
    gdelt_events:     List[dict]
) -> str:
    """
    Build adversarial challenge prompt for Claude.
    Claude receives Agent 3's full assessment and is
    instructed to challenge it — not confirm it.
    """

    # Format key indicators as numbered list
    indicators_text = '\n'.join([
        f'  {i+1}. {ind}'
        for i, ind in enumerate(key_indicators)
    ])

    # Format top GDELT events for source critique
    if gdelt_events:
        events_text = '\n'.join([
            f"  [{e.get('date','?')}] "
            f"[{e.get('data_source','?')}] "
            f"Goldstein={e.get('goldstein_scale',0.0):+.1f}: "
            f"{e.get('title','?')[:80]}"
            for e in gdelt_events[:5]
        ])
    else:
        events_text = '  No event data available.'

    prompt = f"""You are a senior intelligence red team analyst.
Your role is to CHALLENGE the threat assessment below — not confirm it.
Find weaknesses, alternative explanations, and overconfidence.
Be rigorous but intellectually honest — if the assessment is sound, say so.

ANALYST QUERY: {analyst_query}

── AGENT 3 ASSESSMENT TO CHALLENGE ─────────────────────────────────────────

Threat level:      {threat_level}
Threat score:      {threat_score:.3f}
Threat confidence: {threat_confidence}

Threat rationale:
{threat_rationale}

Key indicators cited:
{indicators_text}

── RAW EVIDENCE AGENT 3 USED ────────────────────────────────────────────────

Signal summary (Agent 1):
{signal_summary[:600]}

Historical context (Agent 2):
{historian_summary[:800]}

Top GDELT events by severity:
{events_text}

── YOUR THREE CHALLENGES ────────────────────────────────────────────────────

CHALLENGE 1 — SOURCE CRITIQUE
Are the sources reliable and independent?
- Is GDELT indexing a confirmed event or a single-source rumour?
- How many independent sources reported each key indicator?
- Is there single-source dependency on any critical claim?
- Specifically: are the Bamako siege reports confirmed by
  multiple independent sources or amplified from one report?

CHALLENGE 2 — ALTERNATIVE EXPLANATIONS
Could the same evidence support a different conclusion?
- Are there alternative actors who could explain these events?
- Could Goldstein scores reflect a different event type than
  Agent 3 assumed?
- Is the geographic spread consistent with JNIM specifically
  or could it indicate a different threat actor?

CHALLENGE 3 — CONFIDENCE INFLATION
Is Agent 3 appropriately uncertain?
- Does threat_confidence match actual data quality?
- Are key_indicators specific enough to support threat_level?
- Is absence of KB document sourcing adequately reflected?

── SCORING GUIDANCE ─────────────────────────────────────────────────────────

After challenges, assign a revised score:
- Strong counter_evidence: reduce by 0.10–0.20
- Moderate counter_evidence: reduce by 0.05
- Assessment survives scrutiny: keep same score
- Never increase the score — red team only tempers

Return ONLY a JSON object — no preamble, no markdown fences:

{{
  "red_team_assessment": "2-3 sentences summarising the strongest challenge. Be specific. Reference exact claims being challenged.",
  "counter_evidence": ["specific alternative explanation 1", "specific source weakness 2", "specific overconfidence flag 3"],
  "revised_threat_score": {threat_score:.3f},
  "revised_confidence": "HIGH or MEDIUM or LOW"
}}"""

    return prompt


def challenge_assessment(
    analyst_query:    str,
    signal_summary:   str,
    historian_summary:str,
    threat_level:     str,
    threat_score:     float,
    threat_rationale: str,
    key_indicators:   List[str],
    threat_confidence:str,
    gdelt_events:     List[dict]
) -> dict:
    """
    Main Agent 4 function.
    Only called when threat_score >= 0.7.
    Returns 4 fields for SentinelState.
    """

    prompt = build_red_team_prompt(
        analyst_query     = analyst_query,
        signal_summary    = signal_summary,
        historian_summary = historian_summary,
        threat_level      = threat_level,
        threat_score      = threat_score,
        threat_rationale  = threat_rationale,
        key_indicators    = key_indicators,
        threat_confidence = threat_confidence,
        gdelt_events      = gdelt_events
    )

    client = boto3.client('bedrock-runtime', region_name='us-east-1')

    try:
        response = client.invoke_model(
            modelId     = 'anthropic.claude-3-5-sonnet-20241022-v2:0',
            contentType = 'application/json',
            accept      = 'application/json',
            body        = json.dumps({
                'anthropic_version': 'bedrock-2023-05-31',
                'max_tokens':        800,
                'messages': [
                    {'role': 'user', 'content': prompt}
                ]
            })
        )

        raw    = json.loads(response['body'].read())
        text   = raw['content'][0]['text'].strip()

        # Strip markdown fences defensively
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()

        parsed = json.loads(text)

        # Red team can only temper — never inflate
        revised_score = float(
            parsed.get('revised_threat_score', threat_score)
        )
        revised_score = min(revised_score, threat_score)
        revised_score = max(0.0, revised_score)

        return {
            'red_team_assessment':  parsed.get('red_team_assessment', ''),
            'counter_evidence':     parsed.get('counter_evidence', []),
            'revised_threat_score': round(revised_score, 3),
            'revised_confidence':   parsed.get('revised_confidence', 'LOW')
        }

    except Exception as e:
        print(f'[red_team ERROR] {str(e)}')
        return {
            'red_team_assessment':  f'Red team failed: {str(e)}',
            'counter_evidence':     [],
            'revised_threat_score': round(threat_score, 3),
            'revised_confidence':   threat_confidence
        }
