import boto3
import json

client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')

response = client.invoke_agent(
    agentId='OPABTSHSPN',
    agentAliasId='TSTALIASID',
    sessionId='test-session-001',
    inputText='Find insurgency events in West Africa'
)

full_response = ''
for event in response['completion']:
    if 'chunk' in event:
        full_response += event['chunk']['bytes'].decode('utf-8')

print(full_response)
