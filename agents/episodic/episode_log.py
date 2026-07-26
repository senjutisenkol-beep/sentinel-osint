# ─────────────────────────────────────────────────────────────────────────────
# agents/episodic/episode_log.py
#
# Episodic memory — the system's own run history.
# One JSON object per line (JSONL) appended after every pipeline run.
# Write-only during a run; read back via load_episodes() for analysis.
#
# Purpose: the Day 17 calibration dataset. Each episode records what the
# pipeline saw (channel activation, confidences) and what it decided
# (threat_level/score), tagged by retrieval `condition` so runs can be
# compared. human_quality_score starts null and is filled in later by a
# human rater — that column is the calibration target.
#
# save_episode() NEVER raises — episode logging is a run-completion
# concern and must not be able to break a pipeline run.
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
import uuid
from datetime import datetime, timezone

# NOTE: pandas is imported lazily inside load_episodes(), NOT at module level.
# The write path (save_episode/build_episode) is contracted to never break a
# run, so it must not carry a hard dependency on pandas. Only the analysis
# path (load_episodes) needs a DataFrame.

# Relative to project root (CWD), matching flash_report.py's LOCAL_DIR convention.
LOG_DIR  = 'episodes'
LOG_PATH = os.path.join(LOG_DIR, 'episode_log.jsonl')


def build_episode(result: dict, latency: float, condition: str = 'production') -> dict:
    """
    Build one episode record from a completed pipeline `result` state.

    Captures the inputs the pipeline saw and the decision it made, so the
    episode log doubles as a calibration dataset. report_id is read from
    state — run_pipeline()'s flash-report block sets result['report_id']
    before this is called; a fallback reads it from the flash_report dict.
    """
    report_id = (
        result.get('report_id')
        or result.get('flash_report', {}).get('report_id')
    )

    return {
        'episode_id':            str(uuid.uuid4()),
        'logged_at':             datetime.now(timezone.utc).isoformat(),
        'condition':             condition,
        'report_id':             report_id,
        'session_id':            result.get('session_id'),
        'analyst_query':         result.get('analyst_query'),

        # Agent 1 — signal
        'signal_confidence':     result.get('signal_confidence'),
        'signal_failure_reason': result.get('signal_failure_reason', ''),
        'gdelt_event_count':     len(result.get('gdelt_events', [])),

        # Agent 2 — context channel activation
        'graph_count':           len(result.get('graph_context', [])),
        'temporal_count':        len(result.get('temporal_context', [])),
        'vector_count':          len(result.get('vector_context', [])),
        'context_confidence':    result.get('context_confidence'),

        # Agent 3 — threat decision
        'threat_level':          result.get('threat_level'),
        'threat_score':          result.get('threat_score'),
        'threat_confidence':     result.get('threat_confidence'),

        # Agent 4 — red team (None when it did not run)
        'revised_threat_score':  result.get('revised_threat_score'),

        # Run metadata
        'loop_count':            result.get('loop_count'),
        'latency_seconds':       round(latency, 1) if latency is not None else None,

        # Calibration target — filled in later by a human rater
        'human_quality_score':   None,
    }


def save_episode(result: dict, latency: float, condition: str = 'production') -> None:
    """
    Append one episode to the JSONL log. NEVER raises — a logging failure
    must not break a pipeline run, so all errors are caught and reported.
    """
    try:
        episode = build_episode(result, latency, condition)
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(json.dumps(episode) + '\n')
        print(f'[episode_log] Logged episode {episode["episode_id"]} '
              f'(condition={condition})')
    except Exception as e:
        # Write-only best-effort. Report and move on.
        print(f'[episode_log] WARNING — failed to log episode: {e}')


def load_episodes(condition: str = None):
    """
    Read the episode log into a DataFrame for analysis.
    Optionally filter to one condition. Returns an empty
    DataFrame if the log does not exist yet.
    """
    import pandas as pd  # lazy — only the analysis path needs pandas

    if not os.path.exists(LOG_PATH):
        return pd.DataFrame()

    rows = []
    with open(LOG_PATH, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    # One corrupt line loses one run,
                    # not the whole dataset. Skip it.
                    continue

    df = pd.DataFrame(rows)
    if condition and not df.empty:
        df = df[df['condition'] == condition]
    return df
