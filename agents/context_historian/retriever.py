import pickle
import boto3
import json
import networkx as nx # type: ignore
from typing import List, Dict

# ── GRAPH RETRIEVER ──────────────────────────────────────────────────────────

def load_graph(path: str = 'agents/context_historian/graph/knowledge_graph.pkl') -> nx.DiGraph:
    """Load the NetworkX knowledge graph from disk."""
    with open(path, 'rb') as f:
        return pickle.load(f)


def extract_entities(query: str, G: nx.DiGraph) -> List[str]:
    """Find graph node names that appear in the query string."""
    query_lower = query.lower()
    matched = []
    for node in G.nodes():
        # Replace underscores with spaces for matching — "Northern_Mali" matches "Northern Mali"
        node_readable = node.replace('_', ' ').lower()
        if node_readable in query_lower:
            matched.append(node)
    return matched


def graph_retrieve(query: str, signal_summary: str = '') -> List[str]:
    """
    Traverse the knowledge graph and return relevant relationships as strings.
    Searches both the analyst query and Agent 1's signal summary for entities.
    """
    G = load_graph()
    
    # Search both query and signal summary for entity matches
    combined_text = query + ' ' + signal_summary
    entities = extract_entities(combined_text, G)
    
    # If no entities found, return empty
    if not entities:
        return []
    
    relationships = []
    
    # Edge types we care about — skip generic ones like 'part_of'
    relevant_edge_types = {
        'operates_in', 'affiliated_with', 'borders',
        'intervenes_in', 'supports', 'caused_by'
    }
    
    for entity in entities:
        # Outgoing edges — what this entity does
        for _, neighbor, data in G.out_edges(entity, data=True):
            rel = data.get('relationship', '')
            if rel in relevant_edge_types:
                # Build human-readable string from edge attributes
                parts = [f"{entity} {rel} {neighbor}"]
                if 'since' in data:
                    parts.append(f"since {data['since']}")
                if 'start' in data:
                    parts.append(f"start {data['start']}")
                if 'end' in data:
                    parts.append(f"end {data['end']}")
                if 'operation' in data:
                    parts.append(f"operation {data['operation']}")
                if 'presence_type' in data:
                    parts.append(f"({data['presence_type']})")
                relationships.append(' '.join(parts))
        
        # Incoming edges — what acts upon this entity
        for source, _, data in G.in_edges(entity, data=True):
            rel = data.get('relationship', '')
            if rel in relevant_edge_types:
                parts = [f"{source} {rel} {entity}"]
                if 'since' in data:
                    parts.append(f"since {data['since']}")
                if 'start' in data:
                    parts.append(f"start {data['start']}")
                if 'end' in data:
                    parts.append(f"end {data['end']}")
                relationships.append(' '.join(parts))
    
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for r in relationships:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    
    return unique

# ── TEMPORAL RETRIEVER ───────────────────────────────────────────────────────

def temporal_retrieve(query: str, signal_summary: str = '') -> List[dict]:
    """
    Extract Event nodes from the knowledge graph relevant to the query.
    Returns events sorted chronologically — oldest first.
    This surfaces causal chains that vector RAG cannot reconstruct.
    """
    G = load_graph()

    combined_text = query + ' ' + signal_summary
    entities = extract_entities(combined_text, G)

    # Collect all Event nodes from the graph
    all_events = []
    for node, data in G.nodes(data=True):
        if data.get('type') == 'Event':
            all_events.append({
                'id':          node,
                'date':        data.get('date', ''),
                'description': data.get('description', ''),
                'actors':      data.get('actors', []),
                'region':      data.get('region', '')
            })

    # If no entities matched or event metadata is sparse,
    # return all events sorted chronologically.
    # All Sahel events form relevant historical context for any regional query.
    if not entities:
        return sorted(all_events, key=lambda e: e['date'])

    entity_set = set(e.lower().replace('_', ' ') for e in entities)

    relevant = []
    for event in all_events:
        event_text = (
            event['id'].lower().replace('_', ' ') + ' ' +
            event['description'].lower() + ' ' +
            event['region'].lower().replace('_', ' ') + ' ' +
            ' '.join(a.lower().replace('_', ' ') for a in event['actors'])
        )
        if any(entity in event_text for entity in entity_set):
            relevant.append(event)

    # If filter returns fewer than 5 events, return all events —
    # sparse metadata means we cannot trust the filter
    if len(relevant) < 5:
        return sorted(all_events, key=lambda e: e['date'])

    return sorted(relevant, key=lambda e: e['date'])


# ── VECTOR RETRIEVER ─────────────────────────────────────────────────────────

def vector_retrieve(query: str, signal_summary: str = '') -> List[dict]:
    """
    Query the Bedrock Knowledge Base (WGLUOKITSP) using hybrid search.
    Returns up to 4 chunks with score >= 0.50, formatted for SentinelState.
    
    Combines analyst query + signal summary for richer semantic coverage.
    """
    # Combine query and signal summary — more context = better retrieval
    combined_query = query
    if signal_summary:
        # Take first 300 chars of summary to avoid token limits on the query side
        combined_query = f"{query}. Context: {signal_summary[:300]}"

    client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')

    try:
        response = client.retrieve(
            knowledgeBaseId='WGLUOKITSP',
            retrievalQuery={
                'text': combined_query
            },
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'numberOfResults': 4,
                    'overrideSearchType': 'SEMANTIC'
                }
            }
        )
    except Exception as e:
        print(f"[vector_retrieve ERROR] {str(e)}")
        return [{'content': f'KB retrieval failed: {str(e)}', 'score': 0.0, 'source': 'error'}]
    

    chunks = []
    for result in response.get('retrievalResults', []):
        score = result.get('score', 0.0)

        # Apply minimum score threshold — discard noise
        # Lowered from 0.65 on Day 10 evaluation —
        # 0 chunks returned across 5 queries at 0.65
        if score < 0.50:
            continue

        content = result.get('content', {}).get('text', '')
        # Source URI lives inside location → s3Location → uri
        source = (
            result.get('location', {})
                  .get('s3Location', {})
                  .get('uri', 'unknown')
        )

        chunks.append({
            'content': content,
            'score':   round(score, 3),
            'source':  source
        })

    return chunks