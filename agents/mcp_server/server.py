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
import boto3
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load .env so NEWSAPI_KEY and other credentials are available
# Must happen before importing clients that read os.environ
load_dotenv()

# FastMCP is the high-level Python SDK for building MCP servers.
# It handles all JSON-RPC 2.0 plumbing — you just decorate functions.
from mcp.server.fastmcp import FastMCP

# Import our three data clients
from agents.mcp_server.gdelt_client      import query_gdelt
from agents.mcp_server.gdelt_doc_client  import query_gdelt_doc
from agents.mcp_server.news_client       import query_newsapi
from agents.mcp_server.reliefweb_client  import query_reliefweb


# ── CREATE THE MCP SERVER INSTANCE ───────────────────────────────────────────

# FastMCP takes the server name as its first argument.
# This name appears in capability discovery when a client connects.
mcp = FastMCP('sentinel-geopolitical-mcp')


# ── KB ENRICHMENT CONSTANTS ───────────────────────────────────────────────────

# KB enrichment — write-through from MCP to Bedrock KB
KB_BUCKET      = 'sentinel-osint-knowledge-base'
KB_S3_PREFIX   = 'enrichment-docs/operational'
KB_ID          = 'WGLUOKITSP'
# Find in Bedrock console → KB → Data Sources tab
# Leave as None until you retrieve it from console
KB_DATA_SOURCE_ID = 'SOMSV1H4GQ'


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
def tool_query_gdelt_doc(query: str, region: str = '', days: int = 7) -> str:
    """
    Search GDELT DOC API for recent news articles about a geopolitical topic.

    Searches full article text — proper names like JNIM and Wagner match
    directly. No API key required. Works in Lambda (no server-side blocking).

    Args:
        query:  Search string e.g. 'JNIM insurgency Mali'
        region: Optional region hint e.g. 'Mali'
        days:   How many days back to search (default 7, max 90)

    Returns:
        JSON string containing list of article dicts and a summary count.
    """
    articles = query_gdelt_doc(query=query, region=region, days=days)

    return json.dumps({
        'source':        'GDELT_DOC',
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
    reports = query_reliefweb(query=query, country=country)
    return json.dumps({
        'source':       'ReliefWeb',
        'report_count': len(reports),
        'reports':      reports
    }, indent=2)


# ── KB WRITE-THROUGH ─────────────────────────────────────────────────────────

def persist_to_kb(articles: list, region: str) -> None:
    """
    Persist MCP-fetched articles to S3 and trigger Bedrock KB sync.

    This is the write-through cache pattern:
    MCP fetches data for Agent 1 signal → ALSO saved to KB
    → available as vector context for Agent 2 on future runs.

    Only persists prose articles (GNews, ReliefWeb, GDELT DOC).
    Skips GDELT event TSV rows — CAMEO codes embed poorly.

    Tagged as DOCUMENT_TYPE: operational_signal so Agent 2
    synthesiser knows not to use these as structural context.

    Non-blocking — starts ingestion job and returns immediately.
    New documents available in vector_retrieve() on next run.
    """
    if not articles:
        return

    s3 = boto3.client('s3', region_name='us-east-1')
    uploaded = 0

    for article in articles:
        try:
            doc_id = str(uuid.uuid4())[:8]
            date   = article.get('date', 'unknown')
            source = article.get('data_source', 'unknown').lower()
            key    = (
                f"{KB_S3_PREFIX}/{region.lower().replace(' ','-')}/"
                f"{date}-{source}-{doc_id}.txt"
            )

            # Tag as operational_signal so Agent 2 synthesiser
            # treats it as corroborating signal, not structural context
            text = (
                f"DOCUMENT_TYPE: operational_signal\n"
                f"INGESTION_DATE: {datetime.now(timezone.utc).date().isoformat()}\n"
                f"EVENT_DATE: {date}\n"
                f"REGION: {region}\n"
                f"SOURCE: {article.get('source', '')}\n"
                f"DATA_FEED: {article.get('data_source', '')}\n\n"
                f"TITLE: {article.get('title', '')}\n\n"
                f"{article.get('description', '')}"
            )

            s3.put_object(
                Bucket      = KB_BUCKET,
                Key         = key,
                Body        = text.encode('utf-8'),
                ContentType = 'text/plain'
            )
            uploaded += 1

        except Exception as e:
            print(f'[persist_to_kb] Upload failed: {str(e)}')

    print(f'[persist_to_kb] {uploaded}/{len(articles)} articles saved to S3 for {region}')

    # Trigger KB sync — non-blocking, fire and forget
    # New documents available in vector_retrieve() on next pipeline run
    if KB_DATA_SOURCE_ID:
        try:
            bedrock = boto3.client('bedrock-agent', region_name='us-east-1')
            bedrock.start_ingestion_job(
                knowledgeBaseId = KB_ID,
                dataSourceId    = KB_DATA_SOURCE_ID,
                description     = f'MCP write-through {region} {datetime.now(timezone.utc).date()}'
            )
            print(f'[persist_to_kb] KB sync triggered for {region}')
        except Exception as e:
            print(f'[persist_to_kb] KB sync failed (non-fatal): {str(e)}')
    else:
        print('[persist_to_kb] KB_DATA_SOURCE_ID not set — skipping sync')
        print('Find it: Bedrock console → Knowledge Bases → WGLUOKITSP → Data Sources')


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

    # Build query strings for each news source
    # GDELT DOC: full query string — handles multi-word naturally
    gdelt_doc_query = query if query else ' '.join(keywords)
    # GNews: always OR-join keywords regardless of query parameter —
    # GNews treats spaces as AND so a long query string kills recall.
    # e.g. ['JNIM','Mali','insurgency'] → 'JNIM OR Mali OR insurgency'
    gnews_query = ' OR '.join(keywords) if keywords else query

    # Call GDELT — live structured events
    gdelt_events = query_gdelt(keywords=keywords, region=region)

    # Call GDELT DOC API — full-text article search, no key, no server-side block
    gdelt_doc_articles = query_gdelt_doc(query=gdelt_doc_query, region=region, days=days)

    # Call GNews — full-text article search, works from Lambda, 100 req/day free
    gnews_articles = query_newsapi(query=gnews_query, region=region, days=days)

    reliefweb_reports = query_reliefweb(query=gdelt_doc_query, country=region)

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

    # Normalise GDELT DOC articles
    for a in gdelt_doc_articles:
        normalised.append({
            'date':          a.get('date', 'unknown'),
            'title':         a.get('title', ''),
            'description':   a.get('description', ''),
            'source':        a.get('source', 'GDELT_DOC'),
            'url':           a.get('url', ''),
            'data_source':   'GDELT_DOC',
            'goldstein_scale': 0.0
        })

    # Normalise GNews articles
    for a in gnews_articles:
        normalised.append({
            'date':          a.get('date', 'unknown'),
            'title':         a.get('title', ''),
            'description':   a.get('description', ''),
            'source':        a.get('source', 'GNews'),
            'url':           a.get('url', ''),
            'data_source':   'GNews',
            'goldstein_scale': 0.0
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

    # ── Step 5: Write-through KB enrichment ──────────────────────────────
    # Persist prose articles to S3 for Agent 2 vector channel
    # Skips GDELT event TSV rows (CAMEO codes, no prose)
    # Non-blocking — does not affect this run's output
    articles_to_persist = [
        e for e in deduplicated
        if e.get('data_source') in ('GNews', 'GDELT_DOC', 'ReliefWeb')
        and len(e.get('description', '')) > 100
    ]
    if articles_to_persist:
        try:
            persist_to_kb(articles_to_persist, region or 'global')
        except Exception as e:
            # Never let KB enrichment crash the signal pipeline
            print(f'[query_all_sources] KB persist failed (non-fatal): {str(e)}')

    # ── Step 6: Calculate signal_confidence ──────────────────────────────────
    # Replaces the Lambda's count-based confidence with relevance-aware scoring.
    # This fixes the Wagner bug where 13 irrelevant events = 0.75 confidence.

    gdelt_count      = len(gdelt_events)
    gdelt_doc_count  = len(gdelt_doc_articles)
    gnews_count      = len(gnews_articles)
    reliefweb_count  = len(reliefweb_reports)
    total_events     = len(deduplicated)

    # Count how many sources returned data
    sources_with_data = sum([
        1 if gdelt_count     > 0 else 0,
        1 if gdelt_doc_count > 0 else 0,
        1 if gnews_count     > 0 else 0,
        1 if reliefweb_count > 0 else 0,
    ])

    # Confidence scoring matrix — based on source count and event volume
    if total_events == 0:
        confidence = 0.1
    elif sources_with_data == 1 and total_events <= 5:
        confidence = 0.3
    elif sources_with_data == 1 and total_events > 5:
        confidence = 0.5
    elif sources_with_data == 2:
        confidence = 0.7
    else:
        # 3+ sources corroborating — high confidence
        confidence = 0.9

    # ── Step 7: Build source summary for transparency ─────────────────────────
    source_summary = {
        'GDELT':      gdelt_count,
        'GDELT_DOC':  gdelt_doc_count,
        'GNews':      gnews_count,
        'ReliefWeb':  reliefweb_count,
        'total':      total_events,
        'deduplicated_count': len(deduplicated)
    }

    # ── Step 8: Return merged result ─────────────────────────────────────────
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