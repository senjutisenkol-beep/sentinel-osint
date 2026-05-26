# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/wikidata_client.py
#
# Queries Wikidata SPARQL endpoint for structured geopolitical
# relationships. Used as a dynamic fallback when the local
# knowledge graph returns sparse coverage for a region.
#
# Why Wikidata:
#   - Free, no API key required
#   - Structured relationship data for all countries and actors
#   - Covers Iran, Ukraine, Red Sea and any other region
#     the local graph does not have seeded
#
# All results flagged as verified=False — they are auto-fetched,
# not manually curated like the local graph edges.
# Logged to graph_auto_seeds.json for human review.
#
# Rate limit: 1 request/second — enforced with time.sleep(1)
# SPARQL endpoint: https://query.wikidata.org/sparql
# ─────────────────────────────────────────────────────────────────────────────

import requests
import json
import time
import os
from datetime import datetime
from typing import List

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

# Wikidata public SPARQL endpoint — no auth required
WIKIDATA_SPARQL_URL = 'https://query.wikidata.org/sparql'

# User-Agent required by Wikimedia policy — identifies your application
# Without this Wikidata may block the request
USER_AGENT = 'sentinel-osint-geoint/1.0 (sentinel-osint research pipeline)'

# Path to log auto-seeded relationships for human review
AUTO_SEED_LOG = 'agents/context_historian/graph/graph_auto_seeds.json'

# Maximum relationships to return per query
MAX_RELATIONSHIPS = 20


# ── SPARQL QUERIES ────────────────────────────────────────────────────────────

def build_conflict_query(entity: str) -> str:
    """
    Build a SPARQL query to find geopolitical conflict relationships
    for a given entity (country or armed group).

    Fetches: conflicts, alliances, membership, geographic location,
    sanctions, and diplomatic relationships.
    """
    # Escape single quotes in entity name to prevent SPARQL injection
    entity_safe = entity.replace("'", "\\'")

    return f"""
SELECT DISTINCT ?subjectLabel ?propertyLabel ?objectLabel WHERE {{
  {{
    # Find the entity by English label
    ?subject rdfs:label "{entity_safe}"@en .

    # Get conflict relationships
    {{
      ?subject wdt:P607 ?object .  # conflict
      BIND("involved_in_conflict" AS ?propertyLabel)
    }} UNION {{
      ?subject wdt:P17 ?object .   # country
      BIND("located_in" AS ?propertyLabel)
    }} UNION {{
      ?subject wdt:P710 ?object .  # participant
      BIND("participant" AS ?propertyLabel)
    }} UNION {{
      ?subject wdt:P361 ?object .  # part of
      BIND("part_of" AS ?propertyLabel)
    }} UNION {{
      ?subject wdt:P31 ?object .   # instance of
      BIND("instance_of" AS ?propertyLabel)
    }} UNION {{
      ?subject wdt:P159 ?object .  # headquarters
      BIND("headquartered_in" AS ?propertyLabel)
    }}
  }}

  # Get human-readable labels for all variables
  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
    ?subject rdfs:label ?subjectLabel .
    ?object  rdfs:label ?objectLabel .
  }}
}}
LIMIT {MAX_RELATIONSHIPS}
"""


def build_regional_actors_query(region: str) -> str:
    """
    Build a SPARQL query to find armed groups and political actors
    operating in a given region or country.
    """
    region_safe = region.replace("'", "\\'")

    return f"""
SELECT DISTINCT ?actorLabel ?typeLabel WHERE {{
  # Find armed groups or political parties in this region
  ?actor wdt:P17 ?country .
  ?country rdfs:label "{region_safe}"@en .

  {{
    ?actor wdt:P31 wd:Q1307214 .  # armed organization
    BIND("armed_group" AS ?typeLabel)
  }} UNION {{
    ?actor wdt:P31 wd:Q7210356 .  # political organization
    BIND("political_organization" AS ?typeLabel)
  }} UNION {{
    ?actor wdt:P31 wd:Q4358176 .  # council
    BIND("governing_body" AS ?typeLabel)
  }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
    ?actor rdfs:label ?actorLabel .
  }}
}}
LIMIT 15
"""


# ── MAIN QUERY FUNCTION ───────────────────────────────────────────────────────

def query_wikidata(entity: str, region: str = '') -> List[dict]:
    """
    Query Wikidata for geopolitical relationships for an entity or region.

    Args:
        entity: Primary entity to query e.g. 'Iran', 'IRGC', 'Hamas'
        region: Optional region context e.g. 'Middle East', 'Iran'

    Returns:
        List of relationship dicts with keys:
        source, relationship, target, verified, data_source
        Returns empty list if Wikidata unavailable.
    """

    relationships = []

    # ── Query 1: Conflict relationships for the entity ────────────────────────
    try:
        # Enforce rate limit — Wikimedia policy requires <= 1 req/sec
        time.sleep(1)

        response = requests.get(
            WIKIDATA_SPARQL_URL,
            params={
                'query':  build_conflict_query(entity),
                'format': 'json'
            },
            headers={
                # Required by Wikimedia — identifies your application
                'User-Agent': USER_AGENT,
                'Accept':     'application/sparql-results+json'
            },
            timeout=15
        )
        response.raise_for_status()

        data = response.json()
        results = data.get('results', {}).get('bindings', [])

        for row in results:
            # Extract labels from SPARQL result bindings
            # Each binding has a 'value' key with the actual text
            subject  = row.get('subjectLabel',  {}).get('value', '')
            prop     = row.get('propertyLabel', {}).get('value', '')
            obj      = row.get('objectLabel',   {}).get('value', '')

            # Skip rows where labels are Wikidata IDs (Q12345 format)
            # These are entities without English labels — not useful
            if not subject or not prop or not obj:
                continue
            if subject.startswith('Q') and subject[1:].isdigit():
                continue
            if obj.startswith('Q') and obj[1:].isdigit():
                continue

            relationships.append({
                'source':       subject,
                'relationship': prop,
                'target':       obj,
                'verified':     False,  # Auto-fetched — not manually curated
                'data_source':  'Wikidata'
            })

    except Exception as e:
        print(f'[wikidata_client] Conflict query failed for {entity}: {str(e)}')

    # ── Query 2: Regional actors if region provided ───────────────────────────
    if region and region != entity:
        try:
            time.sleep(1)  # Rate limit

            response = requests.get(
                WIKIDATA_SPARQL_URL,
                params={
                    'query':  build_regional_actors_query(region),
                    'format': 'json'
                },
                headers={
                    'User-Agent': USER_AGENT,
                    'Accept':     'application/sparql-results+json'
                },
                timeout=15
            )
            response.raise_for_status()

            data    = response.json()
            results = data.get('results', {}).get('bindings', [])

            for row in results:
                actor     = row.get('actorLabel', {}).get('value', '')
                actor_type= row.get('typeLabel',  {}).get('value', '')

                if not actor or actor.startswith('Q'):
                    continue

                relationships.append({
                    'source':       actor,
                    'relationship': 'operates_in',
                    'target':       region,
                    'verified':     False,
                    'data_source':  'Wikidata'
                })

        except Exception as e:
            print(f'[wikidata_client] Regional query failed for {region}: {str(e)}')

    # ── Log auto-seeded relationships for human review ────────────────────────
    if relationships:
        _log_auto_seeds(entity=entity, relationships=relationships)

    return relationships


def format_for_graph_context(relationships: List[dict]) -> List[str]:
    """
    Convert Wikidata relationship dicts to human-readable strings
    matching the format graph_retrieve() returns.

    e.g. "Iran involved_in_conflict Iraq War [Wikidata — unverified]"
    """
    formatted = []
    for r in relationships:
        line = (
            f"{r['source']} {r['relationship']} {r['target']} "
            f"[Wikidata — unverified]"
        )
        formatted.append(line)
    return formatted


def _log_auto_seeds(entity: str, relationships: List[dict]) -> None:
    """
    Append auto-seeded relationships to graph_auto_seeds.json
    for human review. Analysts can promote verified entries
    to the main knowledge_graph.pkl in future sessions.
    """
    try:
        # Load existing log or create new
        if os.path.exists(AUTO_SEED_LOG):
            with open(AUTO_SEED_LOG, 'r') as f:
                log = json.load(f)
        else:
            log = {'auto_seeds': []}

        # Append new entry
        log['auto_seeds'].append({
            'timestamp':     datetime.utcnow().isoformat() + 'Z',
            'entity':        entity,
            'relationships': relationships,
            'status':        'pending_review'
        })

        # Save back
        os.makedirs(os.path.dirname(AUTO_SEED_LOG), exist_ok=True)
        with open(AUTO_SEED_LOG, 'w') as f:
            json.dump(log, f, indent=2)

    except Exception as e:
        # Logging failure should never crash the pipeline
        print(f'[wikidata_client] Failed to log auto-seeds: {str(e)}')
