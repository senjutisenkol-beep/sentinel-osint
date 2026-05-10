# ─────────────────────────────────────────────────────────────────
# lambda_handler.py
#
# Bedrock Action Group handler for the Signal Monitor agent.
# This Lambda is now a thin wrapper around the MCP server's
# query_all_sources() function. Bedrock Agent handles intelligent
# query transformation. This handler calls the merged data feed
# and returns results in the Bedrock action group envelope format.
# ─────────────────────────────────────────────────────────────────

import json
import sys
import os

# Add the project root to the path so we can import the MCP server
# When deployed as Lambda, this path is set via Lambda layers or
# packaging — for local testing it finds the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
))))

from dotenv import load_dotenv
load_dotenv()

from agents.mcp_server.server import query_all_sources


def lambda_handler(event, context):
    """
    AWS Lambda handler for Bedrock Agent Action Group.

    Bedrock sends an event containing the tool name and parameters
    that Claude decided to use. We extract those parameters, call
    the MCP server's merged data feed, and return in Bedrock format.

    Event structure from Bedrock:
    {
        "actionGroup": "gdelt-query-action",
        "function": "query-gdelt",
        "parameters": [
            {"name": "keywords", "type": "string", "value": "[\"JNIM\",\"Mali\"]"},
            {"name": "region",   "type": "string", "value": "Mali"}
        ]
    }
    """

    # ── Step 1: Extract parameters from Bedrock event ─────────────────────────

    # Bedrock OpenAPI-schema action groups send parameters inside requestBody,
    # not as a flat 'parameters' list. Extract from the nested structure:
    # event.requestBody.content.application/json.properties → list of {name, value}
    properties = (
        event
        .get('requestBody', {})
        .get('content', {})
        .get('application/json', {})
        .get('properties', [])
    )
    params = {p['name']: p['value'] for p in properties}

    # Extract keywords — Bedrock sends as JSON-encoded string
    # e.g. '["JNIM", "insurgency", "Mali"]' → ['JNIM', 'insurgency', 'Mali']
    keywords_raw = params.get('keywords', '[]')
    try:
        # Claude sends keywords as a JSON array string per our OpenAPI schema
        keywords = json.loads(keywords_raw)
        if isinstance(keywords, str):
            # Sometimes Claude sends a plain string — split it
            keywords = [k.strip() for k in keywords.split(',')]
    except (json.JSONDecodeError, TypeError):
        # Fallback: treat the raw string as a single keyword
        keywords = [keywords_raw] if keywords_raw else []

    # Extract region — plain string
    region = params.get('region', '')

    # Build a natural language query from keywords for NewsAPI + ReliefWeb
    # e.g. ['JNIM', 'Mali', 'insurgency'] → 'JNIM Mali insurgency'
    query = ' '.join(keywords) + (f' {region}' if region else '')

    # ── Step 2: Call the MCP server merged feed ───────────────────────────────

    try:
        # query_all_sources() returns a JSON string
        raw_result = query_all_sources(
            keywords = keywords,
            region   = region,
            query    = query.strip(),
            days     = 7
        )
        merged = json.loads(raw_result)

        # Extract the fields Agent 1 needs to return
        events           = merged.get('events', [])
        signal_confidence= merged.get('signal_confidence', 0.1)
        source_summary   = merged.get('source_summary', {})

        # Build a signal summary string that Agent 2 will use for
        # query expansion in vector retrieval
        signal_summary = (
            f"Signal query: {query.strip()}\n"
            f"Sources: GDELT={source_summary.get('GDELT',0)}, "
            f"NewsAPI={source_summary.get('NewsAPI',0)}, "
            f"ReliefWeb={source_summary.get('ReliefWeb',0)}.\n"
            f"Unique items: {source_summary.get('deduplicated_count',0)}\n\n"
        )

        # Append top 3 event descriptions for richer Agent 2 context
        for e in events[:3]:
            signal_summary += (
                f"[{e.get('date','?')}] "
                f"[{e.get('data_source','?')}] "
                f"{e.get('title','?')}: "
                f"{e.get('description','')[:200]}\n"
            )

        # Build the structured result in the format Agent 1 returns
        result = {
            'events':           events,
            'confidence_score': signal_confidence,
            'query': {
                'keywords_used': keywords,
                'region_filter': region,
                'total_found':   len(events),
                'source_summary': source_summary
            },
            'signal_summary': signal_summary
        }

        # ── Step 3: Return in Bedrock OpenAPI action group envelope format ──────

        # The Bedrock Agent was configured with an OpenAPI schema action group.
        # OpenAPI-schema action groups require this exact response structure —
        # apiPath and httpMethod must echo the values from the incoming event,
        # and the body goes inside responseBody.application/json.body.
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup':   event.get('actionGroup', ''),
                'apiPath':       event.get('apiPath', '/query-gdelt'),
                'httpMethod':    event.get('httpMethod', 'POST'),
                'httpStatusCode': 200,
                'responseBody': {
                    'application/json': {
                        'body': json.dumps(result)
                    }
                }
            }
        }

    except Exception as e:
        # Return a safe error response in Bedrock OpenAPI envelope format
        # so the agent can report the failure rather than crashing
        error_result = {
            'events':           [],
            'confidence_score': 0.1,
            'error':            str(e),
            'query': {
                'keywords_used': keywords,
                'region_filter': region,
                'total_found':   0
            }
        }
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup':   event.get('actionGroup', ''),
                'apiPath':       event.get('apiPath', '/query-gdelt'),
                'httpMethod':    event.get('httpMethod', 'POST'),
                'httpStatusCode': 500,
                'responseBody': {
                    'application/json': {
                        'body': json.dumps(error_result)
                    }
                }
            }
        }
