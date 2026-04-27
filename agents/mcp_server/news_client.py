# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/news_client.py
#
# Queries NewsAPI for recent news articles matching a geopolitical query.
# NewsAPI searches English-language sources including BBC, Reuters, AP,
# Al Jazeera, The Guardian, and thousands more.
#
# Fills two GDELT gaps:
#   1. Named actor search — "JNIM" appears in headlines as written
#   2. Article descriptions — prose context for the synthesiser
# ─────────────────────────────────────────────────────────────────────────────

import requests    # HTTP library for calling the NewsAPI endpoint
import os          # Access environment variables (NEWSAPI_KEY)
from datetime import date, timedelta  # Build date range for the query
from typing import List               # Type hint for return annotation


# ── CONSTANTS ─────────────────────────────────────────────────────────────────

# NewsAPI everything endpoint — searches articles across all indexed sources
NEWSAPI_URL = 'https://newsapi.org/v2/everything'

# Maximum articles to return — keeps prompt size manageable
MAX_ARTICLES = 10


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────

def query_newsapi(query: str, region: str = '', days: int = 7) -> List[dict]:
    """
    Search NewsAPI for recent articles matching a geopolitical query.

    Args:
        query:  Search string e.g. 'JNIM insurgency Mali' or 'Wagner Africa'
        region: Optional region hint appended to query e.g. 'Mali'
        days:   How many days back to search (default 7, free tier max 30)

    Returns:
        List of article dicts, each with: date, title, description,
        source, url, data_source.
        Returns empty list if NewsAPI is unavailable or no articles found.
    """

    # ── Step 1: Build the search query string ─────────────────────────────────

    # Combine the main query with the region for more targeted results
    # e.g. query='JNIM insurgency' + region='Mali' → 'JNIM insurgency Mali'
    full_query = query.strip()
    if region and region.lower() not in full_query.lower():
        # Only append region if it's not already in the query string
        # Avoids redundant "Mali Mali" type queries
        full_query = f'{full_query} {region}'

    # ── Step 2: Build the date range ─────────────────────────────────────────

    # NewsAPI free tier allows searching up to 30 days back
    # We default to 7 days to keep results current and relevant
    end_date   = date.today().isoformat()                          # Today: 2026-04-27
    start_date = (date.today() - timedelta(days=days)).isoformat() # 7 days ago

    # ── Step 3: Call the NewsAPI endpoint ────────────────────────────────────

    try:
        response = requests.get(
            NEWSAPI_URL,
            params={
                'q':        full_query,   # The search query string
                'from':     start_date,   # Start of date range
                'to':       end_date,     # End of date range
                'language': 'en',         # English articles only
                'sortBy':   'publishedAt',# Most recent first
                'pageSize': MAX_ARTICLES, # Max articles to return
                'apiKey':   os.environ['NEWSAPI_KEY']  # Auth key from .env
            },
            timeout=10  # Fail fast if NewsAPI is slow
        )
        response.raise_for_status()  # Raise exception on HTTP error

        data = response.json()

        # NewsAPI returns status 'ok' on success, 'error' on failure
        if data.get('status') != 'ok':
            print(f'[news_client] NewsAPI returned status: {data.get("status")}')
            print(f'[news_client] Message: {data.get("message", "unknown error")}')
            return []

    except Exception as e:
        print(f'[news_client ERROR] Failed to call NewsAPI: {str(e)}')
        return []

    # ── Step 4: Parse and clean the articles ─────────────────────────────────

    articles = []

    for article in data.get('articles', []):

        # Extract the publication date — NewsAPI returns ISO 8601 format
        # e.g. "2026-04-27T14:32:00Z" → we take just the date part "2026-04-27"
        published_at = article.get('publishedAt', '')
        date_str = published_at[:10] if published_at else 'unknown'

        # Extract the source name — e.g. "BBC News", "Reuters", "Al Jazeera"
        source_name = article.get('source', {}).get('name', 'Unknown')

        # Extract description — 1-2 sentence summary of the article
        # Fall back to title if no description available
        description = article.get('description') or article.get('title', '')

        # Truncate very long descriptions to keep prompt size manageable
        if description and len(description) > 300:
            description = description[:300] + '...'

        # Skip articles with no useful content
        if not description:
            continue

        # Build clean event dict — same shape as GDELT output for merger compatibility
        articles.append({
            'date':        date_str,
            'title':       article.get('title', ''),
            'description': description,
            'source':      source_name,
            'url':         article.get('url', ''),
            'data_source': 'NewsAPI'  # Tag so merger knows where this came from
        })

    return articles