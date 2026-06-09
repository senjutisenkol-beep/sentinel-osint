import boto3
import json
from typing import List

# ── CONTEXT SYNTHESISER ──────────────────────────────────────────────────────

def build_synthesis_prompt(
    query: str,
    signal_summary: str,
    graph_context: List[str],
    temporal_context: List[dict],
    vector_context: List[dict]
) -> str:
    """
    Assemble the three retrieval channels into a structured prompt
    for Claude to synthesise into a single analyst summary.
    """

    # Format graph relationships — already human-readable strings
    graph_block = '\n'.join(graph_context) if graph_context else 'No graph relationships retrieved.'

    # Format temporal events — convert dicts to readable lines
    if temporal_context:
        temporal_lines = []
        for e in temporal_context:
            date = e.get('date', 'unknown date')
            desc = e.get('description', '') or e.get('id', 'no description')
            actors = ', '.join(e.get('actors', [])) or 'unknown actors'
            temporal_lines.append(f"[{date}] {desc} (actors: {actors})")
        temporal_block = '\n'.join(temporal_lines)
    else:
        temporal_block = 'No temporal events retrieved.'

    # Format vector chunks — include score and source for traceability
    if vector_context:
        has_operational = any(
            'DOCUMENT_TYPE: operational_signal' in chunk.get('content', '')
            for chunk in vector_context
        )
        operational_note = (
            'NOTE: Some document chunks are tagged operational_signal — '
            'these are recent MCP-fetched articles already covered by '
            'Agent 1 signal channel. Use them as corroborating signal '
            'only, not as structural historical context.\n\n'
            if has_operational else ''
        )
        vector_lines = []
        for i, chunk in enumerate(vector_context, 1):
            score = chunk.get('score', 0.0)
            source = chunk.get('source', 'unknown')
            content = chunk.get('content', '')
            # Truncate very long chunks to keep prompt size manageable
            if len(content) > 600:
                content = content[:600] + '...'
            vector_lines.append(f"[Chunk {i} | score={score} | source={source}]\n{content}")
        vector_block = operational_note + '\n\n'.join(vector_lines)
    else:
        vector_block = 'No document chunks retrieved.'

    prompt = f"""You are a geopolitical intelligence analyst. Your task is to synthesise multi-source context about a security situation and produce a concise intelligence summary.

ANALYST QUERY: {query}

AGENT 1 SIGNAL SUMMARY:
{signal_summary[:500] if signal_summary else 'No signal summary available.'}

--- RETRIEVED CONTEXT ---

GRAPH RELATIONSHIPS (structured knowledge):
{graph_block}

CHRONOLOGICAL EVENTS (temporal chain):
{temporal_block}

DOCUMENT CONTEXT (KB chunks):
{vector_block}

--- INSTRUCTIONS ---

Based on all three context channels above, produce a JSON object with exactly these two keys:

{{
  "summary": "<2-3 paragraph intelligence assessment. Paragraph 1: current situation based on signal. Paragraph 2: historical context from graph and events. Paragraph 3: what the document evidence adds. Be specific — name actors, dates, locations. Do not fabricate details not present in the context.>",
  "confidence": <float between 0.0 and 1.0. Base this on: how many channels returned data, whether channels are consistent with each other, and whether the data is directly relevant to the query. 0.9+ means all three channels returned strong relevant data. 0.5 means only one channel had useful data. 0.2 means context was sparse or irrelevant.>
}}

Return ONLY the JSON object. No preamble, no explanation, no markdown fences."""

    return prompt


def synthesise(
    query: str,
    signal_summary: str,
    graph_context: List[str],
    temporal_context: List[dict],
    vector_context: List[dict]
) -> dict:
    """
    Call Claude Sonnet via Bedrock to synthesise all three retrieval
    channels into a single historian_summary + context_confidence.

    Returns: {'historian_summary': str, 'context_confidence': float}
    """
    prompt = build_synthesis_prompt(
        query, signal_summary, graph_context, temporal_context, vector_context
    )

    client = boto3.client('bedrock-runtime', region_name='us-east-1')

    try:
        response = client.invoke_model(
            modelId='us.anthropic.claude-sonnet-4-6',
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                'anthropic_version': 'bedrock-2023-05-31',
                'max_tokens': 1000,
                'messages': [
                    {'role': 'user', 'content': prompt}
                ]
            })
        )

        raw = json.loads(response['body'].read())
        text = raw['content'][0]['text'].strip()

        # Strip markdown fences if Claude adds them despite instructions
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()

        parsed = json.loads(text)

        return {
            'historian_summary':  parsed.get('summary', 'Synthesis failed — no summary returned.'),
            'context_confidence': float(parsed.get('confidence', 0.1))
        }

    except Exception as e:
        return {
            'historian_summary':  f'ContextSynthesiser failed: {str(e)}',
            'context_confidence': 0.0
        }