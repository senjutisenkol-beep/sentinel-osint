# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/news_client.py
#
# Queries GNews API for recent news articles about a geopolitical topic.
# Replaced NewsAPI (newsapi.org) which restricts free tier to localhost only —
# making it unusable from AWS Lambda. GNews has no localhost restriction.
#
# GNews API: https://gnews.io
# Free tier: 100 requests/day, no localhost restriction
# Auth: API key as query parameter (GNEWS_API_KEY in .env)
# ─────────────────────────────────────────────────────────────────────────────

import requests
import os
from datetime import datetime, timedelta, timezone
from typing import List

# ── CONSTANTS ─────────────────────────────────────────────────────────────────

# GNews API endpoint — searches articles across all indexed sources
GNEWS_URL = 'https://gnews.io/api/v4/search'

# Maximum articles to return
MAX_ARTICLES = 10


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────

def query_newsapi(query: str, region: str = '', days: int = 7) -> List[dict]:
    """
    Search GNews for recent articles matching a geopolitical query.
    Function name kept as query_newsapi() for backwards compatibility
    with server.py and pipeline.py which import this name.

    Args:
        query:  Search string e.g. 'JNIM insurgency Mali'
        region: Optional region hint appended to query
        days:   How many days back to search (default 7)

    Returns:
        List of article dicts with: date, title, description,
        source, url, data_source.
    """

    # ── Step 1: Build search query ────────────────────────────────────────────
    full_query = query.strip()
    if region and region.lower() not in full_query.lower():
        full_query = f'{full_query} {region}'

    # ── Step 2: Build date range ──────────────────────────────────────────────
    # GNews expects ISO 8601 format with timezone
    end_date   = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    # Format: 2026-05-01T00:00:00Z
    from_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    to_str   = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    # ── Step 3: Call GNews API ────────────────────────────────────────────────
    try:
        response = requests.get(
            GNEWS_URL,
            params={
                'q':        full_query,   # Search query
                'from':     from_str,     # Start date
                'to':       to_str,       # End date
                'lang':     'en',         # English articles only
                'sortby':   'publishedAt',# Most recent first
                'max':      MAX_ARTICLES, # Max articles
                'apikey':   os.environ['GNEWS_API_KEY']  # Auth from .env
            },
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        # GNews returns totalArticles count and articles list
        if 'articles' not in data:
            print(f'[news_client] GNews returned no articles key: {data}')
            return []

    except Exception as e:
        print(f'[news_client ERROR] Failed to call GNews: {str(e)}')
        return []

    # ── Step 4: Parse articles ────────────────────────────────────────────────
    articles = []

    for article in data.get('articles', []):

        # GNews returns publishedAt in ISO 8601 format
        published_at = article.get('publishedAt', '')
        date_str = published_at[:10] if published_at else 'unknown'

        # Extract source name
        source_name = article.get('source', {}).get('name', 'Unknown')

        # Extract description — fall back to title if empty
        description = article.get('description') or article.get('title', '')
        if description and len(description) > 300:
            description = description[:300] + '...'

        if not description:
            continue

        articles.append({
            'date':        date_str,
            'title':       article.get('title', ''),
            'description': description,
            'source':      source_name,
            'url':         article.get('url', ''),
            'data_source': 'NewsAPI'  # Keep label for merger compatibility
        })

    return articles
