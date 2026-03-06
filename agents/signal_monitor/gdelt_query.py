import pandas as pd
import requests
from datetime import datetime, timezone
import json
import io

GDELT_MASTER_URL = ( "http://data.gdeltproject.org/gdeltv2/lastupdate.txt")
GDELT_COLUMNS = [f'col_{i}' for i in range(61)]
GDELT_COLUMNS[0]  = 'GLOBALEVENTID'
GDELT_COLUMNS[1]  = 'SQLDATE'
GDELT_COLUMNS[6]  = 'Actor1Name'
GDELT_COLUMNS[16] = 'Actor2Name'
GDELT_COLUMNS[30] = 'GoldsteinScale'
GDELT_COLUMNS[44] = 'ActionGeo_FullName'
GDELT_COLUMNS[45] = 'ActionGeo_CountryCode'
GDELT_COLUMNS[60] = 'SOURCEURL'

USECOLS = [
    'GLOBALEVENTID', 'SQLDATE', 'Actor1Name', 'Actor2Name',
    'GoldsteinScale', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
    'SOURCEURL'
]
def get_latest_gdelt_url() -> str:
    """
    GDELT publishes a master list every 15 minutes.
    The list contains 3 lines — export, mentions, gkg.
    We want the export file (line 1).
    Each line has 3 space-separated fields:
    size  md5hash  url
    We want the url (field 3).
    """
    response = requests.get(GDELT_MASTER_URL, timeout=10)
    response.raise_for_status()
    
    first_line = response.text.strip().split('\n')[0]
    url = first_line.strip().split(' ')[2]
    
    return url
 

def fetch_gdelt_dataframe() -> pd.DataFrame:
    import time
    
    for attempt in range(3):
        try:
            url = get_latest_gdelt_url()
            print(f'Attempt {attempt + 1} — fetching: {url}')
            
            df = pd.read_csv(
                url,
                sep='\t',
                header=None,
                names=GDELT_COLUMNS,
                usecols=USECOLS,
                dtype=str,
                on_bad_lines='skip',
                compression='zip'
            )
            return df
            
        except Exception as e:
            print(f'Attempt {attempt + 1} failed: {e}')
            if attempt < 2:
                time.sleep(10)
            else:
                raise


def query_gdelt(keywords: list, region: str = None, limit: int = 20) -> list:
    """
    Fetches latest GDELT export, filters by keywords and 
    optional region, returns structured event list.
    
    Args:
        keywords: list of search terms (seed + analyst keyword)
        region:   optional geographic filter string
        limit:    maximum events to return (default 20)
    
    Returns:
        list of event dictionaries
    """
    df = fetch_gdelt_dataframe()
    
    # Build keyword filter — match ANY keyword in description fields
    keyword_pattern = '|'.join([k.lower() for k in keywords])
    
    # Search across actor names and location fields
    mask = (
        df['Actor1Name'].str.lower().str.contains(
            keyword_pattern, na=False, regex=True) |
        df['Actor2Name'].str.lower().str.contains(
            keyword_pattern, na=False, regex=True) |
        df['ActionGeo_FullName'].str.lower().str.contains(
            keyword_pattern, na=False, regex=True)
    )
    
    df_filtered = df[mask].copy()
    
    # Apply region filter if provided
    if region:
        region_mask = (
            df_filtered['ActionGeo_FullName'].str.lower().str.contains(
                region.lower(), na=False) |
            df_filtered['ActionGeo_CountryCode'].str.lower().str.contains(
                region.lower(), na=False)
        )
        df_filtered = df_filtered[region_mask]
    
    # Sort by date descending, take top N
    df_filtered = df_filtered.sort_values(
        'SQLDATE', ascending=False
    ).head(limit)
    
    # Build structured output
    events = []
    for _, row in df_filtered.iterrows():
        events.append({
            'date': format_date(row['SQLDATE']),
            'country': row['ActionGeo_CountryCode'],
            'location': row['ActionGeo_FullName'],
            'actors_involved': [
                str(a).lower() for a in [row['Actor1Name'], row['Actor2Name']]
                if str(a).lower() not in ['nan', 'none', '']
            ],
            'event_description': (
                f"Event involving "
                f"{'unknown actor' if str(row['Actor1Name']).lower() in ['nan', 'none', ''] else str(row['Actor1Name']).lower()}"
                f" in "
                f"{'unknown location' if str(row['ActionGeo_FullName']).lower() in ['nan', 'none', ''] else str(row['ActionGeo_FullName']).lower()}"
            ),
            'event_state': None,
            'goldstein_scale': float(row['GoldsteinScale']) 
                               if row['GoldsteinScale'] != 'nan' 
                               else None,
            'source_url': row['SOURCEURL'] 
                          if row['SOURCEURL'] != 'nan' 
                          else None
        })
    
    return events
def format_date(sqldate: str) -> str:
    """
    Converts GDELT SQLDATE format (YYYYMMDD) 
    to ISO 8601 format (YYYY-MM-DD).
    
    Args:
        sqldate: date string in YYYYMMDD format e.g. "20240315"
    
    Returns:
        date string in YYYY-MM-DD format e.g. "2024-03-15"
    """
    try:
        return datetime.strptime(
            str(sqldate), '%Y%m%d'
        ).strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return str(sqldate)
def calculate_confidence(events: list) -> float:
    """
    Scores the quality of the GDELT signal on a 0.0 to 1.0 scale.

    Components:
      0.1  — base score (empty results, pipeline never gets zero)
      0.4  — at least 1 event returned
      +0.2 — 5 or more events (volume signal)
      +0.15 — most recent event within 24 hours (recency signal)
      +0.25 — average Goldstein scale <= -4 (consistent destabilising signal)

    Max possible: 1.0
    Min possible: 0.1
    """
    # Start at 0.1 — even empty results have minimal confidence
    # This ensures the pipeline never receives a zero score
    confidence = 0.1

    if len(events) > 0:

        # Base score — we have at least something to work with
        confidence = 0.4

        # Volume bonus — 5+ events suggests a pattern, not noise
        if len(events) >= 5:
            confidence += 0.2

        # Recency bonus — how old is the most recent event?
        today = datetime.now(timezone.utc).date()
        most_recent = max(events, key=lambda e: e['date'])['date']
        most_recent_date = datetime.strptime(most_recent, '%Y-%m-%d').date()
        days_gap = (today - most_recent_date).days
        if days_gap <= 1:
            confidence += 0.15

        # Goldstein bonus — consistent destabilising signal
        # Filter out None values first — some GDELT events have no score
        goldstein_values = [
            e['goldstein_scale'] for e in events
            if e['goldstein_scale'] is not None
        ]
        if len(goldstein_values) > 0:
            avg_goldstein = sum(goldstein_values) / len(goldstein_values)
            if avg_goldstein <= -4:
                confidence += 0.25

    return round(confidence, 2)


def run_query(keywords: list, region: str = None) -> dict:
    """
    Public interface for lambda_handler.
    Wraps query_gdelt with metadata and error handling.
    
    Args:
        keywords: list of search terms including seed vocabulary
        region:   optional geographic filter
    
    Returns:
        complete response dict matching Agent 1 output schema
    """
    retrieved_at = datetime.now(timezone.utc).isoformat()
    
    try:
        events = query_gdelt(keywords=keywords, region=region)

        return {
            'events': events,
            'confidence_score': calculate_confidence(events),
            'query': {
                'keywords_used': keywords,
                'region_filter': region.lower() if region else None,
                'total_found': len(events),
                'retrieved_at': retrieved_at
            }
        }
    
    except requests.Timeout:
        return {
            'events': [],
            'query': {
                'keywords_used': keywords,
                'region_filter': region.lower() if region else None,
                'total_found': 0,
                'retrieved_at': retrieved_at
            },
            'error': 'GDELT data source timed out. Try again in 60 seconds.'
        }
    
    except requests.HTTPError as e:
        return {
            'events': [],
            'query': {
                'keywords_used': keywords,
                'region_filter': region.lower() if region else None,
                'total_found': 0,
                'retrieved_at': retrieved_at
            },
            'error': f'GDELT data source unavailable: {str(e)}'
        }
    
    except Exception as e:
        return {
            'events': [],
            'query': {
                'keywords_used': keywords,
                'region_filter': region.lower() if region else None,
                'total_found': 0,
                'retrieved_at': retrieved_at
            },
            'error': f'Unexpected error: {str(e)}'
        }