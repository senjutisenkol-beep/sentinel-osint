import json
import boto3
from langgraph.graph import StateGraph, END  # Importing stategraph which is Lang graph's graph editor and end marker
from orchestration.state import SentinelState  # This is the state.py file that carries the briefing document for every agent to read and understand the output from each other

# This function takes a SentinelState dictionary as input and returns a plain dict of changed fields.

def signal_monitor_node(state: SentinelState) -> dict:  # state:SentinelState means the function is a type hint. it tells python that state argument must be a SentinelState type dict
    client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')
    try:
        response = client.invoke_agent(
            agentId='OPABTSHSPN',
            agentAliasId='TSTALIASID',
            sessionId=state['session_id'],
            inputText=state['analyst_query']  # analyst query is directly read from analyst query
        )
        # collecting streaming response from the agent and parsing it as json
        raw = ''
        for events in response['completion']:  # for every streaming response bedrock sends back
            if 'chunk' in events:  # check if response has a chunk/text
                raw += events['chunk']['bytes'].decode('utf-8')  # decode and add it to raw which will contain the complete response from agent 1

        # Find the JSON block more robustly
        json_start = raw.find('{')
        brace_count = 0
        json_end = json_start
        for i in range(json_start, len(raw)):
            if raw[i] == '{':
                brace_count += 1
            elif raw[i] == '}':
                brace_count -= 1
            if brace_count == 0:
                json_end = i + 1
                break
        json_block = raw[json_start:json_end]
        parsed = json.loads(json_block)
        return {
            'gdelt_events': parsed.get('events', []),  # getting gdelt events from the response and if not present return empty list
            'signal_confidence': parsed.get('confidence_score', 0.1),  # getting signal confidence from the response and if not present return 0.1
            'signal_summary': raw,
            'errors': []
        }
    except Exception as e:
        return {
            'gdelt_events':      [],
            'signal_confidence': 0.1,
            'signal_summary':    '',
            'errors':            [f'signal_monitor_node failed: {str(e)}']
        }


def route_after_signal(state: SentinelState) -> str:
    if state['signal_confidence'] >= 0.4:
        return 'context_historian'
    else:
        return 'clarification'


def build_pipeline():
    graph = StateGraph(SentinelState)  # creating a state graph with SentinelState as the state type

    # Register nodes
    graph.add_node('signal_monitor', signal_monitor_node)

    # Set entry point
    graph.set_entry_point('signal_monitor')

    # Add conditional routing after signal monitor node
    graph.add_conditional_edges(
        'signal_monitor',
        route_after_signal,
        {
            'context_historian': END,
            'clarification': END
        }
    )
    return graph.compile()


pipeline = build_pipeline()
