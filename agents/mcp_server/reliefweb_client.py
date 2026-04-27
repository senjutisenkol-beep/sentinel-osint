# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/reliefweb_client.py
#
# Queries the ReliefWeb API for humanitarian and conflict situation reports.
# ReliefWeb is operated by UN OCHA and aggregates reports from UN agencies,
# NGOs, and governments worldwide. No API key or registration required.
#
# Why this instead of ACLED:
#   - ACLED changed to paid-only API access in 2025
#   - ReliefWeb provides prose situation reports (document-level intelligence)
#     rather than just structured event codes
#   - The synthesiser can use report summaries as document evidence when the
#     Pinecone KB vector channel returns no chunks
#   - Completely free with no rate limits for reasonable use
# ─────────────────────────────────────────────────────────────────────────────

import requests    # HTTP library for calling the ReliefWeb API
from typing import List  # Type hint for return annotation


# ── CONSTANTS ─────────────────────────────────────────────────────────────────

# ReliefWeb API base URL — v1 is the stable production endpoint
RELIEFWEB_URL = 'https://api.reliefweb.int/v1/reports'

# App name is required by ReliefWeb to identify the calling application
# It is not a secret — just a label for their logs
APP_NAME = 'sentinel-osint'

# Maximum reports to return
MAX_REPORTS = 10


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────

def query_reliefweb(query: str, country: str = '') -> List[dict]:
    """
    Search ReliefWeb for recent humanitarian and conflict situation reports.

    Args:
        query:   Search string e.g. 'JNIM insurgency' or 'Wagner Mali'
        country: Optional country filter e.g. 'Mali' — narrows results

    Returns:
        List of report dicts, each with: date, title, summary, source,
        url, country, data_source.
        Returns empty list if ReliefWeb is unavailable or no reports found.
    """

    # ── Step 1: Build the request payload ────────────────────────────────────

    # ReliefWeb uses a POST request with a JSON body rather than URL params
    # This allows complex queries — field selection, sorting, filtering
    payload = {

        # appname identifies your application in ReliefWeb's logs
        # Required field — without it the API may throttle or reject requests
        'appname': APP_NAME,

        # query searches across title, body, and source fields
        'query': {
            'value':  query,    # The search string e.g. 'JNIM Mali insurgency'
            'fields': ['title', 'body', 'source.name']  # Fields to search across
        },

        # fields specifies which data to return in each result
        # 'body-html' is the full report — we request 'body' for plain text
        'fields': {
            'include': [
                'title',        # Report headline
                'date',         # Publication date
                'source',       # Organisation that published it (e.g. OCHA, UNHCR)
                'url',          # Direct link to the report
                'country',      # Country or countries the report covers
                'body'          # Full report text (we'll truncate to summary)
            ]
        },

        # sort by date descending — most recent reports first
        'sort': ['date:desc'],

        # limit caps the number of results returned
        'limit': MAX_REPORTS,

        # profile full returns complete field data
        # profile minimal returns only IDs — we need full for summaries
        'profile': 'full'
    }

    # Add country filter if provided — narrows results geographically
    if country:
        # ReliefWeb filter syntax: filter on country.name field
        payload['filter'] = {
            'field': 'country.name',
            'value': country
        }

    # ── Step 2: Call the ReliefWeb API ───────────────────────────────────────

    try:
        response = requests.post(
            RELIEFWEB_URL,
            json=payload,      # Send payload as JSON body
            timeout=15         # ReliefWeb can be slower than other APIs
        )
        response.raise_for_status()  # Raise exception on HTTP error
        data = response.json()

    except Exception as e:
        print(f'[reliefweb_client ERROR] Failed to call ReliefWeb API: {str(e)}')
        return []

    # ── Step 3: Parse the response ────────────────────────────────────────────

    # ReliefWeb response structure:
    # { "data": [ { "id": "...", "fields": { "title": "...", "body": "...", ... } } ] }
    raw_reports = data.get('data', [])

    if not raw_reports:
        print(f'[reliefweb_client] No reports found for query: {query}')
        return []

    reports = []

    for item in raw_reports:

        # All report data lives inside the 'fields' key
        fields = item.get('fields', {})

        # Extract title
        title = fields.get('title', 'No title')

        # Extract date — ReliefWeb returns date as nested dict
        # Structure: { "date": { "created": "2026-04-20T00:00:00+00:00" } }
        date_obj = fields.get('date', {})
        raw_date = date_obj.get('created', '') if isinstance(date_obj, dict) else ''
        # Take just the date part — first 10 chars of ISO 8601 string
        date_str = raw_date[:10] if raw_date else 'unknown'

        # Extract source organisation name
        # Structure: { "source": [ { "name": "OCHA" } ] }
        sources = fields.get('source', [])
        source_name = sources[0].get('name', 'Unknown') if sources else 'Unknown'

        # Extract URL
        url = fields.get('url', '')

        # Extract country names
        # Structure: { "country": [ { "name": "Mali" }, { "name": "Niger" } ] }
        countries = fields.get('country', [])
        country_names = ', '.join(
            c.get('name', '') for c in countries if c.get('name')
        )

        # Extract body text and create a summary
        # Full reports can be thousands of words — we take the first 400 chars
        # This gives the synthesiser enough context without overwhelming the prompt
        body = fields.get('body', '') or ''

        # Strip any HTML tags that may appear in the body text
        # ReliefWeb sometimes includes HTML despite 'body' (not 'body-html') request
        import re
        body_clean = re.sub(r'<[^>]+>', ' ', body)  # Replace HTML tags with spaces
        body_clean = re.sub(r'\s+', ' ', body_clean).strip()  # Collapse whitespace

        # Take first 400 characters as the summary
        summary = body_clean[:400] + '...' if len(body_clean) > 400 else body_clean

        # Fall back to title if body is empty
        if not summary:
            summary = title

        # Build clean report dict — consistent shape with GDELT and NewsAPI
        reports.append({
            'date':        date_str,
            'title':       title,
            'summary':     summary,      # First 400 chars of report body
            'source':      source_name,  # e.g. 'OCHA', 'UNHCR', 'MSF'
            'url':         url,
            'country':     country_names,
            'data_source': 'ReliefWeb'   # Tag so merger knows where this came from
        })

    return reports