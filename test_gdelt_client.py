from agents.mcp_server.gdelt_client import query_gdelt

events = query_gdelt(['Iran'], region='Iran')
print(f'Events found: {len(events)}')
for e in events:
    print(f'  [{e["date"]}] {e["actor1"]} — {e["location"]}')
