import uuid
from orchestration.pipeline import pipeline

# Build initial state
initial_state = {
    'session_id':        str(uuid.uuid4()),
    'analyst_query':     'Find conflict events in Iran',
    'retrieved_at':      '2026-03-09T00:00:00Z',
    'gdelt_events':      [],
    'signal_confidence': 0.0,
    'signal_summary':    '',
    'abort':             False,
    'abort_reason':      '',
    'errors':            []
}

# Run pipeline
result = pipeline.invoke(initial_state)

# Print results
print(f"Confidence:  {result['signal_confidence']}")
print(f"Events:      {len(result['gdelt_events'])} found")
print(f"Errors:      {result['errors']}")
print(f"\nSummary:\n{result['signal_summary'][:500]}")