# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/gdelt_doc_client.py
#
# Queries the GDELT DOC API v2 for recent news articles.
#
# Why this replaces NewsAPI:
#   - NewsAPI free tier blocks server-side requests (AWS Lambda → 426 error)
#   - GDELT DOC API has no key, no server-side restriction, works in Lambda
#   - Searches full article text — proper names like "JNIM" and "Wagner"
#     match because they appear in headlines, unlike the Events API which
#     only has CAMEO actor codes
#
# Difference from gdelt_client.py (Events API):
#   gdelt_client.py  → structured event records (actor, Goldstein, CAMEO code)
#   gdelt_doc_client.py → article list (title, URL, domain, date)
#   Both come from GDELT but are different endpoints and data products.
#
# GDELT DOC API reference:
#   https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
# ─────────────────────────────────────────────────────────────────────────────

import requests
import time
from datetime import datetime
from typing import List


# ── CONSTANTS ─────────────────────────────────────────────────────────────────

GDELT_DOC_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'

# Wikimedia policy requires a User-Agent — GDELT follows the same convention
USER_AGENT = 'sentinel-osint-geoint/1.0 (sentinel-osint research pipeline)'

# Maximum articles to return — keeps prompt size manageable
MAX_RECORDS = 10


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────

def query_gdelt_doc(query: str, region: str = '', days: int = 7) -> List[dict]:
    """
    Search GDELT DOC API for recent news articles matching a query.

    Args:
        query:  Search string e.g. 'JNIM insurgency' or 'Wagner Mali'
        region: Optional region appended to query e.g. 'Mali'
        days:   How many days back to search (1–90)

    Returns:
        List of article dicts with: date, title, description,
        source, url, data_source.
        Returns empty list if unavailable or no articles found.
    """

    # ── Step 1: Build the search query ───────────────────────────────────────

    full_query = query.strip()
    if region and region.lower() not in full_query.lower():
        # Append region only if not already present — avoids "Mali Mali"
        full_query = f'{full_query} {region}'

    # GDELT DOC timespan format: '7days', '30days', max '90days'
    timespan = f'{min(days, 90)}days'

    # ── Step 2: Call the GDELT DOC API (retry once on 429) ───────────────────

    # GDELT DOC enforces 1 request per 5 seconds.
    # Retry once with a 6s sleep rather than sleeping on every call —
    # avoids adding 5s to every pipeline run in the normal case.
    params = {
        'query':      full_query,
        'mode':       'ArtList',      # Return article list (not timeline)
        'maxrecords': MAX_RECORDS,
        'timespan':   timespan,
        'sourcelang': 'english',      # English sources only
        'sort':       'DateDesc',     # Most recent first
        'format':     'json'
    }

    try:
        response = requests.get(
            GDELT_DOC_URL,
            params=params,
            headers={'User-Agent': USER_AGENT},
            timeout=15
        )

        if response.status_code == 429:
            print('[gdelt_doc_client] Rate limited — retrying after 6s')
            time.sleep(6)
            response = requests.get(
                GDELT_DOC_URL,
                params=params,
                headers={'User-Agent': USER_AGENT},
                timeout=15
            )

        response.raise_for_status()
        data = response.json()

    except Exception as e:
        print(f'[gdelt_doc_client ERROR] {str(e)}')
        return []

    # ── Step 3: Parse articles ────────────────────────────────────────────────

    articles = []

    for item in data.get('articles', []):

        # GDELT DOC seendate format: '20260606T150000Z' → '2026-06-06'
        seendate = item.get('seendate', '')
        try:
            date_str = datetime.strptime(seendate, '%Y%m%dT%H%M%SZ').strftime('%Y-%m-%d')
        except ValueError:
            date_str = seendate[:10] if len(seendate) >= 10 else 'unknown'

        title  = item.get('title', '')
        domain = item.get('domain', 'unknown')
        url    = item.get('url', '')

        if not title:
            continue

        articles.append({
            'date':        date_str,
            'title':       title,
            # ArtList mode returns no body excerpt — title carries the signal
            'description': title,
            'source':      domain,
            'url':         url,
            'data_source': 'GDELT_DOC'
        })

    return articles
