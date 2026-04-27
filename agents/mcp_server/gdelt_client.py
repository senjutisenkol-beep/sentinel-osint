# ─────────────────────────────────────────────────────────────────────────────
# agents/mcp_server/gdelt_client.py
#
# Fetches live conflict events from the GDELT 2.0 dataset.
# GDELT (Global Database of Events Language and Tone) indexes news articles
# from thousands of sources worldwide, updated every 15 minutes.
#
# This is the same data source Agent 1's Lambda used, but extracted into a
# clean standalone function with no Bedrock or Lambda wrapping code.
# ─────────────────────────────────────────────────────────────────────────────

import requests   # HTTP library — makes GET requests to GDELT URLs
import zipfile    # GDELT ships its data as .zip files — we need to unzip them
import io         # Lets us treat downloaded bytes as a file object in memory
import csv        # GDELT data is TSV (tab-separated values) — csv handles this
import re         # Regular expressions — used for keyword matching in event text
from datetime import datetime  # For converting GDELT timestamps to readable dates
from typing import List        # Type hint for return type annotation


# ── CONSTANTS ─────────────────────────────────────────────────────────────────

# GDELT publishes a text file every 15 minutes containing the URL of the
# latest event data snapshot. We fetch this first to get the current data URL.
GDELT_LASTUPDATE_URL = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'

# The GDELT 2.0 event file has 61 columns. These are the column indices
# (0-based) we care about — actor names, location, date, event code.
# Column reference: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
GDELT_COLUMNS = {
    'GlobalEventID': 0,    # Unique event identifier
    'Day':           1,    # Date in YYYYMMDD format
    'Actor1Name':    6,    # Name of the first actor (e.g. "JNIM", "FRANCE")
    'Actor2Name':    16,   # Name of the second actor
    'EventCode':     26,   # CAMEO event code (e.g. 190 = Use conventional military force)
    'GoldsteinScale':30,   # Conflict intensity score: -10 (violent) to +10 (cooperative)
    'ActionGeo_FullName': 44,  # Full location name where event occurred
    'SourceURL':     60,   # URL of the source news article
}

# Maximum number of events to return — keeps the prompt size manageable
MAX_EVENTS = 20


# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────

def query_gdelt(keywords: List[str], region: str = '') -> List[dict]:
    """
    Fetch the latest GDELT event snapshot and filter by keywords and region.

    Args:
        keywords: List of search terms e.g. ['JNIM', 'insurgency', 'Mali']
        region:   Optional region filter e.g. 'Mali' — narrows results geographically

    Returns:
        List of event dicts, each with: date, actor1, actor2, event_code,
        goldstein_scale, location, source_url, data_source.
        Returns empty list if GDELT is unavailable or no events match.
    """

    # ── Step 1: Find the latest GDELT snapshot URL ────────────────────────────
    try:
        # lastupdate.txt contains three lines — one per file type (events, mentions, gkg)
        # Each line has a file size, MD5 hash, and URL separated by spaces
        # Example line: 3471167 abc123 http://data.gdeltproject.org/.../20260424.export.CSV.zip
        update_response = requests.get(GDELT_LASTUPDATE_URL, timeout=10)
        update_response.raise_for_status()  # Raises exception if HTTP error (4xx, 5xx)

        # Split the file into lines and find the export CSV line
        # The export file (not mentions or gkg) contains the event data we want
        lines = update_response.text.strip().split('\n')
        export_url = None
        for line in lines:
            # The export file URL contains 'export.CSV.zip' — the other files don't
            if 'export.CSV.zip' in line:
                # URL is the third element on the line after splitting by spaces
                export_url = line.split(' ')[2].strip()
                break

        if not export_url:
            # lastupdate.txt didn't contain an export URL — GDELT may be updating
            print('[gdelt_client] Could not find export URL in lastupdate.txt')
            return []

    except Exception as e:
        # Network error, timeout, or GDELT server down
        print(f'[gdelt_client ERROR] Failed to fetch lastupdate.txt: {str(e)}')
        return []

    # ── Step 2: Download and unzip the GDELT snapshot ────────────────────────
    try:
        # Download the zip file — GDELT snapshots are typically 3–8MB
        zip_response = requests.get(export_url, timeout=30)
        zip_response.raise_for_status()

        # io.BytesIO wraps the raw bytes in a file-like object so zipfile can read it
        # without writing anything to disk — all happens in memory
        zip_bytes = io.BytesIO(zip_response.content)

        # Open the zip archive and read the first (only) file inside it
        with zipfile.ZipFile(zip_bytes) as zf:
            filename = zf.namelist()[0]      # Name of the CSV file inside the zip
            raw_bytes = zf.read(filename)    # Read raw bytes of the CSV

        # Decode bytes to string — GDELT uses latin-1 encoding (not UTF-8)
        # latin-1 handles special characters in non-English news sources
        raw_text = raw_bytes.decode('latin-1')

    except Exception as e:
        print(f'[gdelt_client ERROR] Failed to download/unzip GDELT snapshot: {str(e)}')
        return []

    # ── Step 3: Parse TSV and filter by keywords ──────────────────────────────
    try:
        # GDELT files are tab-separated — csv.reader handles this cleanly
        reader = csv.reader(io.StringIO(raw_text), delimiter='\t')

        # Build the keyword regex pattern — case-insensitive, matches any keyword
        # re.escape() prevents keywords like "C.I.A." from breaking the regex
        # '|'.join() creates an OR pattern: "JNIM|insurgency|Mali"
        if keywords:
            pattern = re.compile(
                '|'.join(re.escape(kw) for kw in keywords),
                re.IGNORECASE  # Match regardless of capitalisation
            )
        else:
            pattern = None  # No keywords = no filtering = return all events

        # Prepare the region filter — lowercase for case-insensitive comparison
        region_lower = region.lower().strip() if region else ''

        events = []  # Accumulate matching events here

        for row in reader:
            # Skip malformed rows that don't have all 61 columns
            if len(row) < 58:
                continue

            # Extract the columns we care about using the index map
            actor1   = row[GDELT_COLUMNS['Actor1Name']]
            actor2   = row[GDELT_COLUMNS['Actor2Name']]
            location = row[GDELT_COLUMNS['ActionGeo_FullName']]
            day_raw  = row[GDELT_COLUMNS['Day']]
            goldstein= row[GDELT_COLUMNS['GoldsteinScale']]
            event_code = row[GDELT_COLUMNS['EventCode']]
            source_url = row[GDELT_COLUMNS['SourceURL']]

            # Combine the text fields we'll search for keyword matches
            searchable_text = f'{actor1} {actor2} {location}'

            # Apply keyword filter — skip this row if no keywords match
            if pattern and not pattern.search(searchable_text):
                continue

            # Apply region filter — skip if location doesn't mention the region
            if region_lower and region_lower not in location.lower():
                continue

            # Convert GDELT date format YYYYMMDD to readable YYYY-MM-DD
            try:
                date_str = datetime.strptime(day_raw, '%Y%m%d').strftime('%Y-%m-%d')
            except ValueError:
                date_str = day_raw  # Keep raw format if parsing fails

            # Convert Goldstein scale to float — default 0.0 if missing
            try:
                goldstein_float = float(goldstein)
            except ValueError:
                goldstein_float = 0.0

            # Append clean event dict — consistent shape with ACLED and NewsAPI output
            events.append({
                'date':            date_str,
                'actor1':          actor1 or 'Unknown',
                'actor2':          actor2 or 'Unknown',
                'event_code':      event_code,
                'goldstein_scale': goldstein_float,
                'location':        location or 'Unknown',
                'source_url':      source_url,
                'data_source':     'GDELT'  # Tag so merger knows where this came from
            })

            # Stop once we have enough events — avoids processing the entire file
            if len(events) >= MAX_EVENTS:
                break

        return events

    except Exception as e:
        print(f'[gdelt_client ERROR] Failed to parse GDELT TSV: {str(e)}')
        return []