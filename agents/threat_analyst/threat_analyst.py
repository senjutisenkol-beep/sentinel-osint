# ─────────────────────────────────────────────────────────────────────────────
# agents/threat_analyst/threat_analyst.py
#
# Agent 3 — Threat Analyst
#
# Architecture:
#   Step 1: Python calculates weighted evidence score from structured signals
#   Step 2: Python estimates uncertainty range from data volume
#   Step 3: Claude makes categorical threat judgment from full context
#
# Scoring approach: heuristic weighted evidence aggregation
# NOT true Bayesian inference (needs historical labelled data we don't have)
# NOT hard threshold rules (cliff edges, context-blind — rejected)
#
# Weights:
#   Signal (Agent 1 live feed):      0.25
#   Context (Agent 2 historical):    0.20
#   Goldstein (violence evidence):   0.55
#
# Goldstein asymmetry (from CLAUDE.md):
#   Negative events (violence)  → full weight upward
#   Positive events (diplomacy) → 0.5x weight downward
#   Rationale: one violent event is stronger evidence of danger
#   than one diplomatic event is evidence of safety
# ─────────────────────────────────────────────────────────────────────────────

import json
import boto3
from typing import List

# ── WEIGHTS ───────────────────────────────────────────────────────────────────
# Judgment-based for now. Replace with logistic regression coefficients
# once 50+ labelled pipeline runs are available (Week 4+).
W_SIGNAL    = 0.25
W_CONTEXT   = 0.20
W_GOLDSTEIN = 0.55

# Positive Goldstein events get half weight — asymmetric by design
POSITIVE_GOLDSTEIN_DISCOUNT = 0.5


def calculate_weighted_score(
    signal_confidence:  float,
    context_confidence: float,
    gdelt_events:       List[dict]
) -> tuple:
    """
    Calculate weighted evidence score and uncertainty range.

    Returns:
        tuple: (weighted_score, score_low, score_high, top_events)
    """

    # Start from neutral prior — no information either way
    base_score = 0.5

    # Signal component — live data above 0.5 pushes threat up
    signal_component = W_SIGNAL * (signal_confidence - 0.5)

    # Context component — strong historical context confirms the picture
    context_component = W_CONTEXT * (context_confidence - 0.5)

    # Goldstein component — direct violence evidence, highest weight
    goldstein_component = 0.0
    top_events = []

    # Split events into Goldstein-carrying and non-carrying
    # Only GDELT provides Goldstein scores
    # ReliefWeb and NewsAPI always return goldstein_scale=0.0
    # Using total event count for uncertainty when most events
    # have no Goldstein data produces misleading narrow bands
    goldstein_events = [
        e for e in gdelt_events
        if float(e.get('goldstein_scale', 0.0)) != 0.0
    ]
    non_goldstein_events = [
        e for e in gdelt_events
        if float(e.get('goldstein_scale', 0.0)) == 0.0
    ]

    if goldstein_events:
        for event in goldstein_events:
            g = float(event.get('goldstein_scale', 0.0))
            if g < 0:
                goldstein_component += abs(g) / 10.0
            elif g > 0:
                goldstein_component -= (g / 10.0) * POSITIVE_GOLDSTEIN_DISCOUNT

        # Normalise by Goldstein-carrying count only
        goldstein_component = (
            W_GOLDSTEIN * goldstein_component / len(goldstein_events)
        )

        # Collect top events for Claude context
        for event in goldstein_events:
            top_events.append({
                'goldstein': float(event.get('goldstein_scale', 0.0)),
                'title':     event.get('title', 'Unknown')[:100],
                'date':      event.get('date', 'unknown'),
                'source':    event.get('data_source', 'unknown')
            })

        top_events.sort(key=lambda x: abs(x['goldstein']), reverse=True)
        top_events = top_events[:5]

    # If no Goldstein events — Goldstein component stays 0.0
    # and Claude will be told explicitly in the prompt

    # Combine all components
    weighted_score = base_score + signal_component + context_component + goldstein_component

    # Clamp to valid range
    weighted_score = max(0.0, min(1.0, weighted_score))

    # Use Goldstein-carrying event count for uncertainty
    # Total event count is misleading when ReliefWeb dominates
    n_goldstein = len(goldstein_events)
    n_total     = len(gdelt_events)

    # Count active channels
    n_sources = sum([
        1 if signal_confidence  > 0.2 else 0,
        1 if context_confidence > 0.3 else 0,
        1 if n_goldstein        > 0   else 0,  # only if Goldstein data exists
    ])

    if n_goldstein >= 10 and n_sources == 3:
        uncertainty = 0.08
    elif n_goldstein >= 5 or n_sources == 2:
        uncertainty = 0.15
    elif n_goldstein >= 1:
        uncertainty = 0.22
    else:
        # No Goldstein data at all — very wide band regardless
        # of how many ReliefWeb/NewsAPI events exist
        uncertainty = 0.30

    score_low  = max(0.0, weighted_score - uncertainty)
    score_high = min(1.0, weighted_score + uncertainty)

    return weighted_score, score_low, score_high, top_events, n_goldstein


def build_threat_prompt(
    analyst_query:      str,
    weighted_score:     float,
    score_low:          float,
    score_high:         float,
    top_events:         list,
    signal_confidence:  float,
    context_confidence: float,
    historian_summary:  str,
    n_events:           int,
    n_goldstein:        int
) -> str:
    """
    Build the prompt for Claude's threat level judgment.
    Claude receives the score AND uncertainty range —
    makes analytical judgment, not hard threshold decisions.
    """

    if top_events:
        events_text = '\n'.join([
            f"  [{e['date']}] [{e['source']}] "
            f"Goldstein={e['goldstein']:+.1f}: {e['title']}"
            for e in top_events
        ])
    else:
        events_text = '  No events with Goldstein data available.'

    non_goldstein_count = n_events - n_goldstein
    if n_goldstein == 0:
        goldstein_warning = (
            "\nWARNING: Zero events carry Goldstein scale data. "
            "All events are from ReliefWeb or NewsAPI which do not "
            "provide Goldstein scores. The 55% Goldstein weight "
            f"contributed NOTHING to this score. The threat_score "
            f"of {weighted_score:.2f} is based only on signal and "
            "context confidence. Do NOT infer conflict intensity "
            "from the absence of Goldstein data — this is a data "
            "gap, not an information blackout."
        )
    else:
        goldstein_warning = (
            f"\n{n_goldstein} of {n_events} "
            f"events carry Goldstein data. "
            f"{non_goldstein_count} ReliefWeb/NewsAPI events "
            f"contributed no Goldstein signal."
        )

    prompt = f"""You are a senior geopolitical intelligence analyst.

ANALYST QUERY: {analyst_query}

── QUANTITATIVE SIGNAL ──────────────────────────────────────────

Weighted Evidence Score: {weighted_score:.2f}
Uncertainty Range:       {score_low:.2f} to {score_high:.2f}
(wider range = less data = lower confidence in the estimate)

Signal confidence (Agent 1 live feed):   {signal_confidence:.2f}
Context confidence (Agent 2 historical): {context_confidence:.2f}
Total events retrieved:                  {n_events}

Most significant events by Goldstein scale
(negative = destabilising/violent, positive = cooperative):
{events_text}
{goldstein_warning}

── HISTORICAL AND STRUCTURAL CONTEXT ───────────────────────────

{historian_summary}

── YOUR TASK ────────────────────────────────────────────────────

Produce a structured threat assessment. Consider:
1. The score AND its uncertainty range — a wide range means
   less certainty regardless of the point estimate value
2. The Goldstein events — severity, recency, geographic spread
3. The historian summary — consistency with known actor behaviour
4. Trend direction — escalating or de-escalating?
5. Actor capabilities — does history support this threat level?

Do NOT use hard thresholds. A score of 0.74 is not automatically
MEDIUM just because it is below 0.75. Use analytical judgment.

Return ONLY a JSON object — no preamble, no markdown fences:

{{
  "threat_level": "HIGH or MEDIUM or LOW or UNKNOWN",
  "threat_rationale": "2-3 sentences explaining your judgment. Reference specific evidence. Acknowledge uncertainty if range is wide.",
  "key_indicators": ["evidence item 1", "evidence item 2", "evidence item 3"],
  "threat_confidence": "HIGH or MEDIUM or LOW — confidence in this assessment based on data quality not threat level"
}}"""

    return prompt


def assess_threat(
    analyst_query:      str,
    signal_confidence:  float,
    context_confidence: float,
    gdelt_events:       List[dict],
    historian_summary:  str
) -> dict:
    """
    Main Agent 3 function — produces structured threat assessment.

    Step 1: Python calculates weighted score + uncertainty
    Step 2: Claude makes categorical judgment from full context
    Step 3: Returns dict for SentinelState

    Returns dict with keys:
        threat_level, threat_score, threat_rationale,
        key_indicators, threat_confidence
    """

    # Step 1 — Calculate weighted score
    weighted_score, score_low, score_high, top_events, n_goldstein = \
        calculate_weighted_score(
            signal_confidence  = signal_confidence,
            context_confidence = context_confidence,
            gdelt_events       = gdelt_events
        )

    # Step 2 — Build prompt for Claude
    prompt = build_threat_prompt(
        analyst_query      = analyst_query,
        weighted_score     = weighted_score,
        score_low          = score_low,
        score_high         = score_high,
        top_events         = top_events,
        signal_confidence  = signal_confidence,
        context_confidence = context_confidence,
        historian_summary  = historian_summary,
        n_events           = len(gdelt_events),
        n_goldstein        = n_goldstein
    )

    # Step 3 — Call Claude via invoke_model()
    # Direct model call — no Bedrock Agent needed
    # All data is pre-fetched, this is pure reasoning
    client = boto3.client('bedrock-runtime', region_name='us-east-1')

    try:
        response = client.invoke_model(
            modelId     = 'us.anthropic.claude-sonnet-4-6',
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

        # Read streaming body
        raw    = json.loads(response['body'].read())
        text   = raw['content'][0]['text'].strip()

        # Strip markdown fences if Claude adds them
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()

        parsed = json.loads(text)

        return {
            'threat_level':      parsed.get('threat_level',     'UNKNOWN'),
            'threat_score':      round(weighted_score, 3),
            'threat_rationale':  parsed.get('threat_rationale',  ''),
            'key_indicators':    parsed.get('key_indicators',    []),
            'threat_confidence': parsed.get('threat_confidence', 'LOW')
        }

    except Exception as e:
        print(f'[threat_analyst ERROR] {str(e)}')
        return {
            'threat_level':      'UNKNOWN',
            'threat_score':      round(weighted_score, 3),
            'threat_rationale':  f'Assessment failed: {str(e)}',
            'key_indicators':    [],
            'threat_confidence': 'LOW'
        }
