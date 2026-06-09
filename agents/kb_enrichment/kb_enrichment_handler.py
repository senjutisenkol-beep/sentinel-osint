# ─────────────────────────────────────────────────────────────────────────────
# agents/kb_enrichment/kb_enrichment_handler.py
#
# Periodic KB enrichment job — runs weekly via EventBridge cron.
# Completely independent of the live pipeline query cycle.
#
# Why periodic not on-demand:
#   1. Rate limits — on-demand sharing quota with live signal
#      pipeline depletes free tier keys quickly
#   2. Bedrock ingestion takes 2-5 min — cannot help current run
#   3. 7-day lag — prevents volatile breaking news contaminating
#      structural KB context. Only settled reporting ingested.
#
# Two S3 prefixes:
#   enrichment-docs/operational/ ← MCP write-through (today's articles)
#   enrichment-docs/structural/  ← this job (7-day lag, stable context)
#
# Agent 2 synthesiser treats structural/ as background knowledge.
# Agent 2 treats operational/ as corroborating signal only —
# UNTIL the article ages past 7 days, at which point retriever.py
# graduates it to structural_context automatically.
# ─────────────────────────────────────────────────────────────────────────────

import os           # Read environment variables (API keys)
import re           # Strip HTML tags from ReliefWeb body text
import json         # Serialise Lambda return value
import uuid         # Generate unique S3 document IDs
import boto3        # AWS SDK — S3 upload and Bedrock KB sync
import requests     # HTTP calls to GNews and ReliefWeb
from datetime import date, timedelta, datetime, timezone
from typing import List
from dotenv import load_dotenv

# Load .env for local testing — no-op in Lambda runtime
load_dotenv()


# ── CONSTANTS ─────────────────────────────────────────────────────────────────

KB_BUCKET         = 'sentinel-osint-knowledge-base'  # S3 bucket for KB docs
KB_S3_PREFIX      = 'documents/structural'            # Prefix for this job's docs
KB_ID             = 'WGLUOKITSP'                     # Bedrock KB ID
KB_DATA_SOURCE_ID = 'SOMSV1H4GQ'                     # Bedrock data source ID

# Only ingest articles older than this many days.
# Prevents live operational signal contaminating structural context.
# After 7 days, articles are considered settled reporting.
LAG_DAYS = 7

# Regions enriched on every weekly run.
# Add new regions here as analyst query coverage expands.
DEFAULT_REGIONS = [
    'Mali', 'Niger', 'Burkina Faso',
    'Iran', 'Gaza', 'Ukraine',
    'Sudan', 'Somalia',
    'Wagner Group Africa', 'Sahel insurgency'
]

MAX_ARTICLES_PER_SOURCE = 5   # Cap per region per source — quality over quantity
RELIEFWEB_APP = 'senjutisen-geoint-research-x7k2NBxz56heKW29d'


# ── DATE FILTER ───────────────────────────────────────────────────────────────

def is_old_enough_for_kb(article_date_str: str) -> bool:
    """
    Return True only if article is older than LAG_DAYS (7 days).

    Prevents today's breaking news entering KB as structural context.
    Articles newer than 7 days stay as operational_signal only.
    After 7 days they graduate to structural_context in retriever.py.
    """
    try:
        article_date = date.fromisoformat(article_date_str)
        # Cutoff: today minus lag. Article must be ON or BEFORE cutoff.
        cutoff_date  = date.today() - timedelta(days=LAG_DAYS)
        return article_date <= cutoff_date
    except (ValueError, TypeError):
        # Unparseable date — skip. Safer to miss than ingest unknown recency.
        print(f'[kb_enrichment] Unparseable date: {article_date_str} — skipping')
        return False


# ── GNEWS FETCH ───────────────────────────────────────────────────────────────

def fetch_gnews(region: str) -> List[dict]:
    """
    Fetch settled articles from GNews for a region.

    Uses GNEWS_ENRICHMENT_KEY — dedicated key separate from
    GNEWS_API_KEY used by Agent 1. This preserves Agent 1's
    100 req/day quota for live signal queries.
    Falls back to GNEWS_API_KEY if enrichment key not set.
    """
    # Prefer dedicated enrichment key to protect Agent 1 quota
    key = os.environ.get(
        'GNEWS_ENRICHMENT_KEY',
        os.environ.get('GNEWS_API_KEY', '')
    )
    if not key:
        print(f'[kb_enrichment] No GNews key — skipping GNews for {region}')
        return []

    try:
        response = requests.get(
            'https://gnews.io/api/v4/search',
            params={
                'q':      f'{region} conflict security',
                'lang':   'en',
                'max':    MAX_ARTICLES_PER_SOURCE,
                'sortby': 'publishedAt',
                'apikey': key
            },
            timeout=10
        )
        response.raise_for_status()

        articles = []
        for a in response.json().get('articles', []):

            # Extract date — first 10 chars of ISO 8601 string
            pub_date = a.get('publishedAt', '')[:10]

            # Skip articles newer than 7 days
            if not is_old_enough_for_kb(pub_date):
                continue

            # Use description or fall back to title
            description = a.get('description') or a.get('title', '')

            # Skip articles too short to embed meaningfully
            if len(description) < 100:
                continue

            articles.append({
                'title':       a.get('title', ''),
                'description': description,
                'source':      a.get('source', {}).get('name', 'GNews'),
                'date':        pub_date,
                'url':         a.get('url', ''),
                'data_source': 'GNews'
            })

        print(f'[kb_enrichment] GNews: {len(articles)} articles for {region}')
        return articles

    except Exception as e:
        print(f'[kb_enrichment] GNews failed for {region}: {str(e)}')
        return []


# ── RELIEFWEB FETCH ───────────────────────────────────────────────────────────

def fetch_reliefweb(region: str) -> List[dict]:
    """
    Fetch settled situation reports from ReliefWeb for a region.

    ReliefWeb has no rate limit and no API key — just appname.
    Returns full prose situation reports which embed far better
    than short news articles. Rich content = better vector search.
    """
    try:
        # appname must be URL param not POST body — ReliefWeb v2 requirement
        response = requests.post(
            f'https://api.reliefweb.int/v2/reports?appname={RELIEFWEB_APP}',
            json={
                'query': {
                    'value':  f'{region} conflict security situation',
                    'fields': ['title', 'body', 'source.name']
                },
                'fields': {
                    'include': ['title', 'date', 'source', 'url', 'body']
                },
                'sort':    ['date:desc'],
                'limit':   MAX_ARTICLES_PER_SOURCE,
                'profile': 'full'
            },
            timeout=15
        )
        response.raise_for_status()

        reports = []
        for item in response.json().get('data', []):
            fields = item.get('fields', {})

            # Date is nested: {"date": {"created": "2026-05-10T..."}}
            date_obj = fields.get('date', {})
            pub_date = (
                date_obj.get('created', '')[:10]
                if isinstance(date_obj, dict) else ''
            )

            # Skip articles newer than 7 days
            if not is_old_enough_for_kb(pub_date):
                continue

            # Strip HTML tags from body text
            body       = fields.get('body', '') or ''
            body_clean = re.sub(r'<[^>]+>', ' ', body)
            body_clean = re.sub(r'\s+', ' ', body_clean).strip()

            # Combine title and body — truncate to 2000 chars
            title   = fields.get('title', '')
            content = f"{title}\n\n{body_clean[:2000]}"

            if len(content.strip()) < 100:
                continue

            sources  = fields.get('source', []) or []
            src_name = sources[0].get('name', 'ReliefWeb') if sources else 'ReliefWeb'

            reports.append({
                'title':       title,
                'description': content,
                'source':      src_name,
                'date':        pub_date,
                'url':         fields.get('url', ''),
                'data_source': 'ReliefWeb'
            })

        print(f'[kb_enrichment] ReliefWeb: {len(reports)} reports for {region}')
        return reports

    except Exception as e:
        print(f'[kb_enrichment] ReliefWeb failed for {region}: {str(e)}')
        return []


# ── S3 UPLOAD ─────────────────────────────────────────────────────────────────

def upload_to_s3(articles: List[dict], region: str) -> int:
    """
    Upload articles to S3 as plain text files.

    Tagged DOCUMENT_TYPE: structural_context — these are settled
    articles with 7-day lag. Agent 2 synthesiser uses them freely
    as background historical knowledge without any caveat label.

    Returns count of successfully uploaded files.
    """
    s3       = boto3.client('s3', region_name='us-east-1')
    uploaded = 0

    for article in articles:
        try:
            # Build unique S3 key: structural/mali/2026-05-01-gnews-a3f2.txt
            doc_id      = str(uuid.uuid4())[:8]
            pub_date    = article.get('date', 'unknown')
            source      = article.get('data_source', 'unknown').lower()
            region_slug = region.lower().replace(' ', '-')
            key         = f"{KB_S3_PREFIX}/{region_slug}/{pub_date}-{source}-{doc_id}.txt"

            # DOCUMENT_TYPE: structural_context tells Agent 2 synthesiser
            # this is settled background knowledge — use freely
            # INGESTION_DATE records when this was added to KB
            # EVENT_DATE records when the article was published
            text_content = (
                f"DOCUMENT_TYPE: structural_context\n"
                f"INGESTION_DATE: {datetime.now(timezone.utc).date().isoformat()}\n"
                f"EVENT_DATE: {pub_date}\n"
                f"REGION: {region}\n"
                f"SOURCE: {article.get('source', '')}\n"
                f"DATA_FEED: {article.get('data_source', '')}\n"
                f"URL: {article.get('url', '')}\n\n"
                f"TITLE: {article.get('title', '')}\n\n"
                f"{article.get('description', '')}"
            )

            # ContentType text/plain — Bedrock Nova Lite parser reads as text
            s3.put_object(
                Bucket      = KB_BUCKET,
                Key         = key,
                Body        = text_content.encode('utf-8'),
                ContentType = 'text/plain'
            )
            uploaded += 1
            print(f'[kb_enrichment] Uploaded: s3://{KB_BUCKET}/{key}')

        except Exception as e:
            # Individual upload failure is non-fatal — continue
            print(f'[kb_enrichment] Upload failed: {str(e)}')

    return uploaded


# ── BEDROCK KB SYNC ───────────────────────────────────────────────────────────

def trigger_kb_sync() -> bool:
    """
    Start Bedrock KB ingestion job — fire and forget.

    Bedrock reads all S3 files under the data source prefix,
    chunks new documents, embeds with Titan Embeddings v2,
    and stores vectors in Pinecone. Takes 2-5 minutes.

    We do not wait for completion — new docs available next run.
    """
    bedrock = boto3.client('bedrock-agent', region_name='us-east-1')
    try:
        response = bedrock.start_ingestion_job(
            knowledgeBaseId = KB_ID,
            dataSourceId    = KB_DATA_SOURCE_ID,
            description     = f'Weekly enrichment {datetime.now(timezone.utc).isoformat()}'
        )
        job_id = response['ingestionJob']['ingestionJobId']
        print(f'[kb_enrichment] Ingestion job started: {job_id}')
        return True
    except Exception as e:
        # Non-fatal — docs in S3, sync can be triggered manually
        print(f'[kb_enrichment ERROR] KB sync failed: {str(e)}')
        return False


# ── MAIN ENRICHMENT FUNCTION ──────────────────────────────────────────────────

def enrich_kb(regions: List[str] = None) -> dict:
    """
    Run enrichment for all regions. One KB sync at the end.

    One sync covers all regions — Bedrock ingestion processes
    the entire data source prefix, picking up all new files.
    """
    if not regions:
        regions = DEFAULT_REGIONS

    print(f'[kb_enrichment] Starting — {len(regions)} regions')
    print(f'[kb_enrichment] Lag filter: articles before '
          f'{(date.today() - timedelta(days=LAG_DAYS)).isoformat()}')

    total_uploaded = 0
    region_results = {}

    for region in regions:
        print(f'\n[kb_enrichment] Processing: {region}')

        gnews_docs     = fetch_gnews(region)
        reliefweb_docs = fetch_reliefweb(region)
        all_docs       = gnews_docs + reliefweb_docs

        if not all_docs:
            print(f'[kb_enrichment] No docs for {region}')
            region_results[region] = {'gnews': 0, 'reliefweb': 0, 'uploaded': 0}
            continue

        uploaded = upload_to_s3(all_docs, region)
        total_uploaded += uploaded

        region_results[region] = {
            'gnews':     len(gnews_docs),
            'reliefweb': len(reliefweb_docs),
            'uploaded':  uploaded
        }

    # One sync after all regions uploaded
    sync_started = trigger_kb_sync() if total_uploaded > 0 else False

    summary = {
        'timestamp':      datetime.now(timezone.utc).isoformat(),
        'lag_days':       LAG_DAYS,
        'regions':        regions,
        'total_uploaded': total_uploaded,
        'sync_started':   sync_started,
        'region_results': region_results
    }

    print(f'\n[kb_enrichment] Done: {total_uploaded} uploaded, sync={sync_started}')
    return summary


# ── LAMBDA HANDLER ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Lambda entry point for EventBridge weekly cron.

    EventBridge rule:
        Schedule: cron(0 0 ? * SUN *)   every Sunday 00:00 UTC
        Target: this Lambda
    """
    print('[kb_enrichment] EventBridge cron triggered')
    summary = enrich_kb(regions=DEFAULT_REGIONS)
    return {'statusCode': 200, 'body': json.dumps(summary)}


# ── LOCAL TEST ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('Local test — Mali, Iran, Gaza')
    summary = enrich_kb(regions=['Mali', 'Iran', 'Gaza'])
    print(json.dumps(summary, indent=2))
