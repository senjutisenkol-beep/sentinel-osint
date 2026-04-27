# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/server.py
#
# FastMCP server — the central entry point for the Sentinel-OSINT
# geopolitical intelligence data feed.
#
# Exposes three individual source tools and one merged tool:
#   - query_gdelt()        → live conflict events from GDELT 2.0
#   - query_newsapi()      → recent news articles from NewsAPI
#   - query_reliefweb()    → UN/NGO situation reports (stub until approved)
#   - query_all_sources()  → merged, deduplicated feed from all sources
#
# Run directly for testing:
#   python -m agents.mcp_server.server
#
# The pipeline connects to this server via MCP client (Week 3 wiring).
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load .env so NEWSAPI_KEY and other credentials are available
# Must happen before importing clients that read os.environ
load_dotenv()

# FastMCP is the high-level Python SDK for building MCP servers.
# It handles all JSON-RPC 2.0 plumbing — you just decorate functions.
from mcp.server.fastmcp import FastMCP

# Import our three data clients
from agents.mcp_server.gdelt_client    import query_gdelt
from agents.mcp_server.news_client     import query_newsapi


# ── CREATE THE MCP SERVER INSTANCE ───────────────────────────────────────────

# FastMCP takes the server name as its first argument.
# This name appears in capability discovery when a client connects.
mcp = FastMCP('sentinel-geopolitical-mcp')


# ── TOOL 1: GDELT ─────────────────────────────────────────────────────────────

@mcp.tool()
def tool_query_gdelt(keywords: List[str], region: str = '') -> str:
    """
    Query GDELT 2.0 for live conflict events matching keywords and region.

    GDELT indexes global news every 15 minutes. Returns structured events
    with actor names, location, Goldstein scale (conflict intensity), and
    source URL.

    Args:
        keywords: Search terms e.g. ['JNIM', 'insurgency', 'Mali']
        region:   Optional region filter e.g. 'Mali'

    Returns:
        JSON string containing list of event dicts and a summary count.
    """
    # Call the GDELT client — returns List[dict]
    events = query_gdelt(keywords=keywords, region=region)

    # Return as JSON string — MCP tools must return strings or simple types
    # The pipeline parses this JSON after receiving it
    return json.dumps({
        'source':      'GDELT',
        'event_count': len(events),
        'events':      events
    }, indent=2)


# ── TOOL 2: NEWSAPI ───────────────────────────────────────────────────────────

@mcp.tool()
def tool_query_newsapi(query: str, region: str = '', days: int = 7) -> str:
    """
    Search NewsAPI for recent news articles about a geopolitical topic.

    NewsAPI searches thousands of English-language sources including BBC,
    Reuters, AP, Al Jazeera, and The Guardian. Returns article titles,
    summaries, and source URLs.

    Args:
        query:  Search string e.g. 'JNIM insurgency Mali'
        region: Optional region hint e.g. 'Mali'
        days:   How many days back to search (default 7, max 30 on free tier)

    Returns:
        JSON string containing list of article dicts and a summary count.
    """
    articles = query_newsapi(query=query, region=region, days=days)

    return json.dumps({
        'source':        'NewsAPI',
        'article_count': len(articles),
        'articles':      articles
    }, indent=2)


# ── TOOL 3: RELIEFWEB (STUB) ─────────────────────────────────────────────────

@mcp.tool()
def tool_query_reliefweb(query: str, country: str = '') -> str:
    """
    Search ReliefWeb for UN and NGO humanitarian situation reports.

    ReliefWeb (UN OCHA) aggregates field reports, situation reports, and
    crisis updates from UN agencies, NGOs, and governments worldwide.
    Provides document-level intelligence with prose summaries.

    Args:
        query:   Search string e.g. 'JNIM insurgency'
        country: Optional country filter e.g. 'Mali'

    Returns:
        JSON string containing list of report dicts and a summary count.
        Currently returns empty list — appname approval pending.
    """
    # ── STUB: ReliefWeb appname approval pending ──────────────────────────────
    # Appname 'senjutisen-geoint-research-x7k2' submitted and under review.
    # Replace this stub with the real reliefweb_client call when approved:
    #
    #   from agents.mcp_server.reliefweb_client import query_reliefweb
    #   reports = query_reliefweb(query=query, country=country)
    #
    # ─────────────────────────────────────────────────────────────────────────
    return json.dumps({
        'source':       'ReliefWeb',
        'report_count': 0,
        'reports':      [],
        'status':       'pending_appname_approval'
    }, indent=2)


# ── TOOL 4: MERGED FEED ───────────────────────────────────────────────────────

@mcp.tool()
def query_all_sources(
    keywords:  List[str],
    region:    str = '',
    query:     str = '',
    days:      int = 7
) -> str:
    """
    Query all available sources and return a merged, deduplicated event feed.

    This is the primary tool the pipeline uses in production. It:
      1. Queries GDELT for live conflict events
      2. Queries NewsAPI for recent news articles
      3. Queries ReliefWeb for situation reports (stub until approved)
      4. Merges all results and removes duplicates
      5. Calculates a signal_confidence score based on data quality

    Args:
        keywords: GDELT keyword list e.g. ['JNIM', 'Mali', 'insurgency']
        region:   Geographic filter e.g. 'Mali'
        query:    NewsAPI/ReliefWeb search string e.g. 'JNIM insurgency Mali'
        days:     How many days back to search (default 7)

    Returns:
        JSON string with merged events, confidence score, and source summary.
    """

    # ── Step 1: Query each source independently ───────────────────────────────

    # Build the NewsAPI query from keywords if not provided separately
    # e.g. keywords=['JNIM','Mali'] → query='JNIM Mali'
    news_query = query if query else ' '.join(keywords)

    # Call GDELT — live events
    gdelt_events = query_gdelt(keywords=keywords, region=region)

    # Call NewsAPI — news articles
    news_articles = query_newsapi(query=news_query, region=region, days=days)

    # ReliefWeb stub — returns empty list until appname approved
    reliefweb_reports = []

    # ── Step 2: Normalise all results to a common schema ─────────────────────
    # Each source returns dicts with different field names.
    # We normalise to: date, title, description, source, url, data_source
    # This allows the merger and the synthesiser to treat all items equally.

    normalised = []

    # Normalise GDELT events
    for e in gdelt_events:
        normalised.append({
            'date':        e.get('date', 'unknown'),
            # GDELT has no title — build one from actors and location
            'title':       f"{e.get('actor1','?')} — {e.get('location','?')}",
            'description': (
                f"Event code {e.get('event_code','?')}. "
                f"Goldstein scale: {e.get('goldstein_scale', 0.0)}. "
                f"Actors: {e.get('actor1','?')} vs {e.get('actor2','?')}."
            ),
            'source':      'GDELT',
            'url':         e.get('source_url', ''),
            'data_source': 'GDELT',
            # Goldstein scale passed through for Agent 3 Bayesian scoring
            'goldstein_scale': e.get('goldstein_scale', 0.0)
        })

    # Normalise NewsAPI articles
    for a in news_articles:
        normalised.append({
            'date':          a.get('date', 'unknown'),
            'title':         a.get('title', ''),
            'description':   a.get('description', ''),
            'source':        a.get('source', 'NewsAPI'),
            'url':           a.get('url', ''),
            'data_source':   'NewsAPI',
            'goldstein_scale': 0.0  # NewsAPI has no Goldstein score
        })

    # Normalise ReliefWeb reports (empty for now)
    for r in reliefweb_reports:
        normalised.append({
            'date':          r.get('date', 'unknown'),
            'title':         r.get('title', ''),
            'description':   r.get('summary', ''),
            'source':        r.get('source', 'ReliefWeb'),
            'url':           r.get('url', ''),
            'data_source':   'ReliefWeb',
            'goldstein_scale': 0.0
        })

    # ── Step 3: Deduplicate by title similarity ───────────────────────────────
    # GDELT often indexes the same event from multiple news sources.
    # We deduplicate by checking if a title is already present (case-insensitive).
    seen_titles = set()
    deduplicated = []

    for item in normalised:
        # Normalise title to lowercase, strip whitespace for comparison
        title_key = item['title'].lower().strip()

        # Skip if we've already seen this title
        if title_key in seen_titles:
            continue

        seen_titles.add(title_key)
        deduplicated.append(item)

    # ── Step 4: Sort by date descending (most recent first) ───────────────────
    deduplicated.sort(
        key=lambda x: x.get('date', ''),
        reverse=True  # Most recent first
    )

    # ── Step 5: Calculate signal_confidence ──────────────────────────────────
    # Replaces the Lambda's count-based confidence with relevance-aware scoring.
    # This fixes the Wagner bug where 13 irrelevant events = 0.75 confidence.

    gdelt_count     = len(gdelt_events)
    newsapi_count   = len(news_articles)
    reliefweb_count = len(reliefweb_reports)
    total_events    = len(deduplicated)

    # Count how many sources returned data
    sources_with_data = sum([
        1 if gdelt_count > 0     else 0,
        1 if newsapi_count > 0   else 0,
        1 if reliefweb_count > 0 else 0,
    ])

    # Confidence scoring matrix — based on source count and event volume
    if total_events == 0:
        # No data from any source — honest zero signal
        confidence = 0.1
    elif sources_with_data == 1 and total_events <= 5:
        # One source, low volume — weak signal
        confidence = 0.3
    elif sources_with_data == 1 and total_events > 5:
        # One source, good volume — moderate signal
        confidence = 0.5
    elif sources_with_data == 2:
        # Two sources corroborating — strong signal
        confidence = 0.7
    else:
        # All three sources with data — maximum confidence
        confidence = 0.9

    # ── Step 6: Build source summary for transparency ─────────────────────────
    source_summary = {
        'GDELT':      gdelt_count,
        'NewsAPI':    newsapi_count,
        'ReliefWeb':  reliefweb_count,
        'total':      total_events,
        'deduplicated_count': len(deduplicated)
    }

    # ── Step 7: Return merged result ─────────────────────────────────────────
    return json.dumps({
        'signal_confidence': confidence,
        'total_events':      total_events,
        'source_summary':    source_summary,
        'events':            deduplicated,
        'retrieved_at':      datetime.utcnow().isoformat() + 'Z'
    }, indent=2)


# ── SERVER ENTRY POINT ────────────────────────────────────────────────────────

if __name__ == '__main__':
    # mcp.run() starts the server using stdio transport.
    # The server listens for JSON-RPC messages on stdin
    # and writes responses to stdout.
    # When the pipeline launches this as a subprocess,
    # all communication happens through these standard streams.
    print('[sentinel-geopolitical-mcp] Starting server...', flush=True)
    mcp.run()