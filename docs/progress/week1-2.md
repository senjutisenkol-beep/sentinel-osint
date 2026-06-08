# Sentinel-OSINT — Week 1–2 Build Log

Archived day-by-day execution notes. Not loaded into Claude context automatically.
Reference here when debugging or revisiting past decisions.

---

## Week 1 Days 1–2 — COMPLETE (2026-03-02)
- AWS CLI v2.34.0, boto3 v1.42.59, python-dotenv v1.2.2, pytest v9.0.2 installed
- AWS profile `senjutisen` configured and verified
- GitHub repo live: sentinel-osint, main branch
- Folder structure committed (agents/, infrastructure/, knowledge_base/, evaluation/, step_functions/)
- config.py, requirements.txt, .env.example, shared/ utilities created

---

## Day 3 — COMPLETE (2026-03-02)
- Agent sentinel-signal-monitor created in Bedrock console
- Model: claude-sonnet-4-6 | Agent ID: OPABTSHSPN | Alias ID: TSTALIASID
- Bedrock model access enabled via AWS Marketplace (Model Access page retired)
- claude-sonnet-4-6 requires INFERENCE_PROFILE type (not on-demand)
  ARN: `arn:aws:bedrock:us-east-1:048420889730:inference-profile/us.anthropic.claude-sonnet-4-6`
- Senjutisen IAM needed aws-marketplace:ViewSubscriptions/Subscribe/Unsubscribe in SentinelOSINMasterPolicy

---

## Day 4 — COMPLETE (2026-03-04)
- `agents/signal_monitor/gdelt_query.py` — fetches latest GDELT 2.0 export via lastupdate.txt
  - Reads 61-column TSV, selects 8 cols, filters by keyword regex across Actor1Name/Actor2Name/ActionGeo_FullName
  - Sorted by SQLDATE desc, capped at 20 events
  - Fix 1: actors_involved — `str(a).lower() not in ['nan','none','']` catches float NaN + string 'nan'
  - Fix 2: event_description — 'unknown actor'/'unknown location' substituted when NaN
- `agents/signal_monitor/lambda_handler.py` — Bedrock action group event handler
  - Extracts properties from `requestBody.content.application/json.properties`
  - Prepends seed vocab: ["conflict", "tension", "war", "insurgency"]
  - Wraps response in Bedrock envelope: `{messageVersion, response: {actionGroup, apiPath, httpMethod, httpStatusCode, responseBody}}`
- "West Africa" returns 0 events — GDELT stores specific countries ("Nigeria"), not regional groupings

---

## Day 5 — COMPLETE (2026-03-04)

### Lambda Packaging (Linux Binaries)
Lambda runs Amazon Linux 2 (x86_64). Must use `--platform manylinux2014_x86_64`:
```bash
pip install pandas requests --platform manylinux2014_x86_64 --target ./package \
  --implementation cp --python-version 3.11 --only-binary=:all:
cp gdelt_query.py lambda_handler.py ./package/
cd ./package && Compress-Archive -Path * -DestinationPath ../deployment.zip
```
ZIP: 47MB. Lambda 512MB memory (RAM: pandas~50MB + numpy~30MB + GDELT~20MB + overhead~30MB = ~145MB min).

### IAM Role
```bash
aws iam create-role --role-name sentinel-lambda-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name sentinel-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```
Role ARN: `arn:aws:iam::048420889730:role/sentinel-lambda-role`

### Lambda Deploy
```bash
aws lambda create-function --function-name sentinel-signal-monitor-gdelt \
  --runtime python3.11 --role arn:aws:iam::048420889730:role/sentinel-lambda-role \
  --handler lambda_handler.lambda_handler --zip-file fileb://deployment.zip \
  --timeout 60 --memory-size 512 --region us-east-1
```
Lambda ARN: `arn:aws:lambda:us-east-1:048420889730:function:sentinel-signal-monitor-gdelt`

### Bedrock → Lambda Permission
```bash
aws lambda add-permission --function-name sentinel-signal-monitor-gdelt \
  --statement-id bedrock-invoke-permission --action lambda:InvokeFunction \
  --principal bedrock.amazonaws.com --source-account 048420889730 --region us-east-1
```

### OpenAPI Schema Key Decisions
- `keywords` is `type: string` NOT `type: array` — Bedrock serialisation is inconsistent with arrays
- Description fields are instructions to Claude at inference time, not documentation
- Stored as JSON in git (action_group_schema.json), pasted as YAML into Bedrock inline editor

### Action Group
- Name: gdelt-query-action | Lambda: sentinel-signal-monitor-gdelt
- Validated in Bedrock: Errors 0, Warnings 0. Agent re-prepared: DRAFT → PREPARED

### Data Flow (Critical)
```
Analyst → Bedrock → Claude → Bedrock → Lambda → Bedrock → Claude → Analyst
```
- Bedrock receives query, builds prompt (system + tool definitions from OpenAPI + query), sends to Claude
- Claude outputs tool-call intention text → Bedrock intercepts → Lambda invoked → result fed back

### Errors Fixed
- PowerShell inline JSON for `--payload` → encoding errors. Fix: use `fileb://test_payload.json`
- Windows pandas DLLs crash Lambda. Fix: `--platform manylinux2014_x86_64`. ZIP: 39MB → 47MB
- StatusCode 200 ≠ code succeeded. Always `cat response.json` — FunctionError is there

### Why LangGraph (not Bedrock Supervisor)
Bedrock Supervisor: black box routing, no visibility, can't debug.
LangGraph: explicit TypedDict state, every routing edge is Python, LangGraph Studio debugging.
AWS officially endorses LangGraph + Bedrock (April 2025 blog).

---

## Day 6 — COMPLETE (2026-03-10)

### calculate_confidence() Scoring (gdelt_query.py)
```
0.1  base — always present
+0.3 event base — ≥1 event returned
+0.2 volume — ≥5 events
+0.15 recency — most recent event within 24h
+0.25 goldstein — average Goldstein ≤ -4
Min: 0.1  Max: 1.0
```
Added to all run_query() return paths (success + error paths return 0.1 on failure).
Output casing normalised: actors_involved, event_description, region_filter → lowercase.

### Lambda Redeploy Sequence
```bash
rm -rf ./package && rm -f deployment.zip
pip install pandas requests --platform manylinux2014_x86_64 --target ./package \
  --implementation cp --python-version 3.11 --only-binary=:all:
cp gdelt_query.py ./package/ && cp lambda_handler.py ./package/
cd ./package && powershell -Command "Compress-Archive -Path * -DestinationPath ../deployment.zip -Force"
cd .. && aws lambda update-function-code --function-name sentinel-signal-monitor-gdelt \
  --zip-file fileb://deployment.zip --region us-east-1
```

### SentinelState (orchestration/state.py)
```python
session_id, analyst_query, retrieved_at          # pipeline-level
gdelt_events, signal_confidence, signal_summary  # Agent 1
abort, abort_reason                              # pipeline control
errors                                           # error accumulator
```
LangGraph requires TypedDict. Each node returns only fields it changes; LangGraph merges the rest.

### Pipeline (orchestration/pipeline.py)
- `signal_monitor_node()` — calls invoke_agent, collects stream, brace-counting JSON extraction
- `route_after_signal()` — `>= 0.4` → context_historian, else → clarification
- Brace counting used (not rfind) because Agent 1 returns prose before JSON block

---

## Day 7 — COMPLETE (2026-03-10)

### Knowledge Graph (agents/context_historian/graph/seed_graph.py)
NetworkX DiGraph. 79 nodes, ~105 edges. Sahel/West Africa conflict 2003–2024.

Node types: Region (22), Actor (15), ExternalActor (16), Event (16), Conflict (10)
Edge types: borders, operates_in, affiliated_with, intervenes_in, supports, caused_by, part_of

Key nodes: JNIM (active, 2017), ISGS (active, 2015), Wagner (active 2023), Liptako_Gourma (highest attack density)
Conflict chain: 2003_GSPC → 2007_AQIM → 2017_JNIM | 2011_Libya_Collapse → 2012_Coup → 2013_Serval → 2022_France_Withdrawal → 2023_Wagner

Design: DiGraph (directed — direction encodes meaning). Serialised to `.pkl` (not in git).
Algeria appears as both Region AND ExternalActor (legitimate dual role).

---

## Day 8 Part 1 — Bedrock KB + Pinecone — COMPLETE (2026-03-10)

### Resources Created
- IAM role: `AmazonBedrockExecutionRoleForKnowledgeBase_sentinel`
  ARN: `arn:aws:iam::048420889730:role/AmazonBedrockExecutionRoleForKnowledgeBase_sentinel`
  Inline policy: s3:GetObject/ListBucket, bedrock:InvokeModel, secretsmanager:GetSecretValue,
                 bedrock:GetInferenceProfile, bedrock:InvokeModelWithResponseStream
- Pinecone API key: `sentinel/pinecone-api-key` in Secrets Manager
  ARN: `arn:aws:secretsmanager:us-east-1:048420889730:secret:sentinel/pinecone-api-key-3w4haJ`
- KB: WGLUOKITSP (sentinel-context-historian-kb) | Embedding: amazon.titan-embed-text-v2:0 (1024 dims)
  Pinecone host: `https://sentinel-osint-index-qr67ju4.svc.aped-4627-b74a.pinecone.io`
  Namespace: `geopolitical-history`
- Data Source: SOMSV1H4GQ | Parser: Amazon Nova Lite (`amazon.nova-lite-v1:0`)
  Note: parsingConfiguration cannot be updated — must delete and recreate data source to change parser
- 6 docs indexed (ACLED, SIPRI PDFs, codesria text). Ingestion job K9UET5WSWS: scanned=6, indexed=6

### Parser Selection Journey
Default parser → failed (scanned image PDFs, 0 text). Claude 3 Haiku → not enabled.
Claude Sonnet 4.6 inference profile → rejected (KB requires foundation model ARN).
Amazon Titan Text Premier → end-of-life. Amazon Nova Lite → ACCESSIBLE, accepted.

---

## Day 8 Part 2 — Context Historian 3-Channel Retrieval — COMPLETE (2026-04-19)

### Architecture: Direct Retrieval (not Bedrock Agent)
```
query + signal_summary
  ├─► graph_retrieve()    — NetworkX .pkl traversal
  ├─► temporal_retrieve() — NetworkX .pkl chronological events
  └─► vector_retrieve()   — Bedrock KB WGLUOKITSP retrieve()
                  ↓
         synthesiser.py — invoke_model() → Claude Sonnet → {summary, confidence}
```
Rationale: synthesiser has all inputs pre-prepared. invoke_model = direct HTTPS.
invoke_agent adds ~500ms + session management with no benefit.

### Key Fixes
Fix 1 — temporal sparse fallback: if entity-filter returns <5 events, discard filter, return all 16 chronologically
Fix 2 — HYBRID → SEMANTIC: Pinecone index has dense vectors only; HYBRID requires dense+sparse
  Long-term: recreate index with sparse support when KB >20 docs

### vector_retrieve() Key Details
```python
combined_query = f"{query}. Context: {signal_summary[:300]}"  # 300-char cap
client = boto3.client('bedrock-agent-runtime', ...)  # NOT bedrock-runtime
# overrideSearchType: SEMANTIC (not HYBRID)
# score filter: discard < 0.50 (was 0.65, lowered Day 10)
```

### synthesiser.py Key Details
```python
modelId = 'anthropic.claude-3-5-sonnet-20241022-v2:0'
response_body = json.loads(response['body'].read())  # streaming — must .read() first
# strip markdown fences defensively (Claude wraps in ``` occasionally)
```

### SentinelState additions (Agent 2)
```python
graph_context, temporal_context, vector_context  # retrieval outputs
historian_summary, context_confidence            # synthesis outputs
```
ALL fields must be in initial_state at pipeline.invoke() — even ones Agent 2 will overwrite.

### AWS Debugging Pattern
Always `print(str(e))` in except — reveals full AWS error.
AccessDeniedException → IAM. ResourceNotFoundException → ID + region. ValidationException → config mismatch.

### context_confidence Scale
0.9+ all three channels strong | 0.5 one channel | 0.2 sparse/irrelevant | 0.35 two channels, no live signal

---

## Day 9 — LangSmith Tracing — COMPLETE (2026-04-19)

### Setup
LangSmith EU endpoint used (`https://eu.api.smith.langchain.com`) — standard endpoint returned 403.
load_dotenv() added to pipeline.py (must be before LangGraph graph initialises).
.env formatting fixed — KB_ID line had UTF-16 encoding artifact from earlier edit.

### 5-Query Results
| Query | A1 conf | GDELT | A2 conf | Note |
|-------|---------|-------|---------|------|
| Mali/JNIM | 0.1 | 0 | 0.35 | Clean |
| Red Sea/Houthi | 0.1 | 0 | 0.0 | JSON parse error (throttled) |
| Drone/UAV Sahel | 0.1 | 0 | 0.2 | Good historian summary |
| Wagner Group | 0.75 | 10 | 0.52 | Best run — all components |
| Niger 2023 coup | 0.1 | 0 | 0.45 | A1 empty response error |

### Bottleneck
signal_monitor_node: 28.31s (68%). Total: ~41.66s (target <30s).
Week 3 fix: Step Functions parallel execution saves ~15s.

---

## Day 10 — Evaluation + Calibration — COMPLETE (2026-04-21)

### Threshold Decisions
| Parameter | Old | New | Reason |
|-----------|-----|-----|--------|
| Routing threshold | 0.4 | 0.1 | 0.1 = base score (no events), not failure — Agent 2 should always run |
| Vector score | 0.65 | 0.50 | 0 chunks returned at 0.65; lower to 0.40 after KB grows |

### Graph Channel Performance
Query 3 (Drone/UAV): 55 rels | Query 1 (Mali/JNIM): 36 | Query 5 (Niger): 12 | Query 4 (Wagner): 6 (most precise) | Query 2 (Red Sea): 1 (correct — no nodes)
Precision beats volume: Wagner's 6 relationships produced better synthesis than Drone's 55.

### Known Issues for Week 3
- Niger query reproducible empty response from Bedrock Agent (`Expecting value: line 1 column 1`)
- Synthesiser JSON parse error on Red Sea (partial/throttled response) — add robust extraction
- Vector channel inactive (0 chunks) — MCP KB enrichment is priority

---

## Day 11 — MCP Server Live + Lambda Upgrade — COMPLETE (2026-05-10)

### What Changed
- ReliefWeb client: v1→v2 endpoint, appname moved to URL query param (`?appname=APP_NAME`)
- lambda_handler.py: calls `query_all_sources()` from MCP server (not gdelt_query directly)
  OpenAPI envelope format retained (apiPath + httpStatusCode + responseBody.application/json)
- Lambda redeployed: Linux manylinux2014_x86_64 binaries, 4.8MB (down from 47MB)
  pydantic (Rust extension) must use --only-binary=:all: or it installs Windows .pyd

### MCP Sources Active
- GDELT: live 15-min snapshots
- NewsAPI: 7-day English news
- ReliefWeb: UN OCHA situation reports (v2, appname: senjutisen-geoint-research-x7k2NBxz56heKW29d)

### Lambda as MCP Client (not subprocess)
Lambda imports `query_all_sources()` directly. `mcp.run()` is never called.
MCP server pattern preserved for local testing and future HTTP+SSE (Week 4).

### Best Run to Date
Mali/JNIM: signal_confidence=0.5, 10 GDELT events, 54 graph rels, context_confidence=0.62, errors=[]

### Errors Fixed
- ReliefWeb 410: v1 retired → update to v2
- ReliefWeb 400: v2 requires appname in URL query param
- Lambda crash: Windows pydantic .pyd on Linux → rebuilt with manylinux2014_x86_64
- Bedrock envelope mismatch: function-calling envelope vs OpenAPI envelope → fix to OpenAPI format
