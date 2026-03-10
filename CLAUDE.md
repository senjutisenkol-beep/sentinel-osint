# Sentinel-OSINT — Claude Code Instructions

## Project
Multi-Agent Geopolitical Risk Detection System on AWS Bedrock.
4-week build plan. Goal: build AND learn engineering + scientific complexity.

## AWS
- Region: us-east-1
- Account: 048420889730
- IAM User: Senjutisen
- Profile: senjutisen (configured in ~/.aws)

## Model
claude-sonnet-4-6 (`anthropic.claude-sonnet-4-6-20251001-v1:0`)
Do not use Claude 3.5 Sonnet or any other model version.

## Day 4 — Signal Monitor Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ANALYST / TEST SCRIPT                        │
│              inputText: "Find insurgency events in West Africa"     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  invoke_agent()
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│               AWS BEDROCK AGENT  (sentinel-signal-monitor)          │
│               Agent ID: OPABTSHSPN  │  Alias: TSTALIASID           │
│               Model: claude-sonnet-4-6 (inference profile)         │
│                                                                     │
│  1. Checks query clarity (Rule 1 — clarify before acting)          │
│  2. Applies seed vocab (Rule 3):                                    │
│     ["conflict","tension","war","insurgency"] + analyst keyword     │
│  3. Decides to call action group → /query-gdelt                    │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  Bedrock action group invocation
                          │  { apiPath: /query-gdelt,
                          │    properties: [keywords, region] }
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   lambda_handler.py                                 │
│                                                                     │
│  • Receives Bedrock action group event (messageVersion 1.0)        │
│  • Extracts keywords + region from requestBody properties          │
│  • Parses keywords JSON string → Python list                       │
│  • Merges: seed_vocab + analyst_keywords → combined keyword list   │
│  • Calls run_query(keywords, region)                               │
│  • Wraps result in Bedrock response envelope                       │
│    { messageVersion, response: { httpStatusCode: 200,             │
│      responseBody: { application/json: { body: "..." }}}}         │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  run_query(keywords, region)
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     gdelt_query.py                                  │
│                                                                     │
│  get_latest_gdelt_url()                                            │
│  └─► GET data.gdeltproject.org/gdeltv2/lastupdate.txt              │
│       parse line 1 → latest export file URL                        │
│                          │                                          │
│  fetch_gdelt_dataframe() │                                          │
│  └─► pd.read_csv(url, sep='\t', usecols=[8 cols], dtype=str,      │
│                   compression='zip', on_bad_lines='skip')          │
│       ~100k rows per 15-min GDELT snapshot                         │
│                          │                                          │
│  query_gdelt()           │                                          │
│  ├─► keyword_pattern = regex OR of all keywords                    │
│  ├─► mask: Actor1Name | Actor2Name | ActionGeo_FullName contains   │
│  ├─► region filter (optional secondary mask)                       │
│  ├─► sort by SQLDATE desc → head(20)                               │
│  └─► build events list per row:                                    │
│        date, country, location                                      │
│        actors_involved → str(a).lower() not in [nan,none,'']      │
│        event_description → "unknown actor/location" if NaN        │
│        event_state: null (never inferred)                          │
│        goldstein_scale: raw GDELT value (ALL signs retained)       │
│        source_url                                                   │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  list of ≤20 event dicts
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    run_query() response envelope                    │
│                                                                     │
│  {                                                                  │
│    "events": [ { date, country, location, actors_involved,         │
│                  event_description, event_state,                    │
│                  goldstein_scale, source_url }, ... ],             │
│    "query":  { keywords_used, region_filter,                       │
│                total_found, retrieved_at }                         │
│  }                                                                  │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  back through lambda_handler → Bedrock agent
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│               BEDROCK AGENT — final response to analyst             │
│               Structured JSON only (Rule 5 — no analysis)          │
│               Returned via invoke_agent() completion stream        │
└─────────────────────────────────────────────────────────────────────┘

Goldstein Decision (Week 1):
  ALL signs returned here ──► Agent 3 applies asymmetric weighting (Week 3)
  negative → increases threat score
  positive → reduces threat score × 0.5
```

## Week 1 Decisions

### Agent 1 — Goldstein Filter
Agent 1 returns ALL events regardless of Goldstein sign.
Positive Goldstein events retained for full geopolitical picture.
Rationale: ceasefire, aid, diplomacy events provide context
for Agent 2 and mitigation signals for Agent 3 scoring.

Threat scoring filter applied in Agent 3 only (Week 3).
Agent 3 uses asymmetric Bayesian weighting:
  negative Goldstein → increases threat score
  positive Goldstein → reduces threat score with 0.5x weight

## Build Rules
- Do not write any agent code, Lambda functions, schemas, or infrastructure until explicitly instructed
- Follow the 4-week plan in bedrock-mas-roadmap.html exactly
- Build one week at a time, one day-block at a time

## Environment
- Python 3.14 (Windows)
- Installed: boto3, botocore, python-dotenv, pytest
- Shell: bash via Claude Code (Unix syntax)

## Progress

### Week 1 Days 1–2 — COMPLETE (2026-03-02)
- [x] AWS CLI v2.34.0 installed
- [x] AWS profile `senjutisen` configured and verified (`aws sts get-caller-identity`)
- [x] boto3 v1.42.59 installed
- [x] python-dotenv v1.2.2 installed
- [x] pytest v9.0.2 installed
- [x] GitHub repo live: sentinel-osint, main branch, 3 commits
- [x] Folder structure committed (agents/, infrastructure/, knowledge_base/, evaluation/, step_functions/)
- [x] config.py, requirements.txt, .env.example created
- [x] shared/ utilities: aws_client.py, logger.py, s3_utils.py

### Week 1 Days 3–5 — COMPLETE (2026-03-04)

#### Day 3 — COMPLETE (2026-03-02)
- [x] Agent sentinel-signal-monitor created in Bedrock console
- [x] Model: claude-sonnet-4-6
- [x] Instruction prompt pasted from agents/signal_monitor/instruction_prompt.txt
- [x] Status: PREPARED
- [x] Agent ID: OPABTSHSPN
- [x] Alias ID: TSTALIASID (AgentTestAlias → DRAFT version)
- [x] .env created from .env.example, Agent ID and Alias ID recorded
- [x] Bedrock model access unblocked via AWS Marketplace (see note below)
- [x] Direct model invocation verified: claude-sonnet-4-6 responds via bedrock-runtime
- [x] Agent invocation verified: invoke_agent() via boto3 returns live response
- [x] Instruction prompt Rule 1 confirmed working: agent asked for clarification
      on ambiguous query before acting — correct behaviour

Notes from Day 3:
- claude-sonnet-4-6 only supports INFERENCE_PROFILE inference type (not on-demand)
  Bedrock uses: arn:aws:bedrock:us-east-1:048420889730:inference-profile/us.anthropic.claude-sonnet-4-6
- Model Access page in Bedrock console has been retired — enable model access
  directly through AWS Marketplace going forward
- Senjutisen IAM user needed aws-marketplace:ViewSubscriptions, Subscribe, Unsubscribe
  added to SentinelOSINMasterPolicy (done via root account)

#### Day 4 — COMPLETE (2026-03-04)
- [x] agents/signal_monitor/gdelt_query.py — written and tested
      - Fetches latest GDELT 2.0 export via lastupdate.txt URL resolution
      - Reads 61-column TSV, selects 8 relevant columns only (usecols)
      - Filters rows by keyword match across Actor1Name, Actor2Name,
        ActionGeo_FullName using vectorised pandas str.contains (na=False)
      - Optional region filter applied as secondary mask
      - Sorted by SQLDATE descending, capped at 20 events
      - Returns structured list matching Agent 1 output schema
      - run_query() is the public interface called by lambda_handler
      - Two data quality fixes applied and verified:
        Fix 1 — actors_involved: str(a).lower() not in ['nan','none','']
                 catches both float NaN and string 'nan' from pandas
        Fix 2 — event_description: 'unknown actor' / 'unknown location'
                 substituted when Actor1Name or ActionGeo_FullName is NaN

- [x] agents/signal_monitor/lambda_handler.py — written and tested
      - Receives Bedrock action group event (messageVersion 1.0 format)
      - Extracts properties from requestBody.content.application/json.properties
      - Parses keywords JSON string from request, prepends seed vocabulary:
        ["conflict", "tension", "war", "insurgency"] + analyst keywords
      - Calls run_query(keywords, region) from gdelt_query.py
      - Wraps response in Bedrock-required envelope:
        { messageVersion, response: { actionGroup, apiPath, httpMethod,
          httpStatusCode, responseBody: { application/json: { body: "..." }}}}

- [x] agents/signal_monitor/local_test.py — run and verified locally
      Test keywords fed: ["attack", "military", "killed", "forces"]
      Combined with seed vocab → 8 keywords total in search
      Result:
        Status: 200
        Total events: 20
        Keywords used: ['insurgency','war','attack','military',
                        'tension','conflict','killed','forces']
        First event returned:
          date: 2026-03-04
          country: US
          location: Los Angeles, California, United States
          actors_involved: ["MILITARY"]
          event_description: "Event involving unknown actor in
                              Los Angeles, California, United States"
          event_state: null
          goldstein_scale: 4.0
          source_url: https://www.dailybreeze.com/2026/03/03/...

Notes from Day 4:
- "military" in the result came from the analyst test input, not seed vocab
- NaN fix confirmed: Actor1Name was NaN → showed as "unknown actor" in
  description, and was excluded from actors_involved list correctly
- action_group_schema.json not yet written — carry to Day 5 prep

#### Day 5 — COMPLETE (2026-03-04)

##### Day 5 Execution Sequence (In Order)
```
1.  Package Lambda with Linux binaries (--platform manylinux2014_x86_64)
      pip install pandas requests → ./package/
      copy gdelt_query.py + lambda_handler.py → ./package/
      ZIP from inside ./package/ → deployment.zip (47MB)

2.  Create IAM execution role
      aws iam create-role --role-name sentinel-lambda-role
      aws iam attach-role-policy → AWSLambdaBasicExecutionRole

3.  Deploy Lambda function
      aws lambda create-function
        --function-name sentinel-signal-monitor-gdelt
        --runtime python3.11 --memory-size 512 --timeout 60

4.  Grant Bedrock permission to invoke Lambda
      aws lambda add-permission
        --principal bedrock.amazonaws.com
        --source-account 048420889730

5.  Test Lambda directly via CLI (isolated, before Bedrock)
      Created test_payload.json (simulates Bedrock event format)
      aws lambda invoke --payload fileb://test_payload.json response.json
      cat response.json → Status 200, structured JSON, no FunctionError

6.  Write OpenAPI schema (action_group_schema.json)
      /query-gdelt POST with keywords (string) and region (string)
      keywords type:string not array — Bedrock serialisation contract
      Validated in Bedrock inline editor: Errors 0, Warnings 0

7.  Attach Action Group in Bedrock console
      gdelt-query-action created
      Lambda: sentinel-signal-monitor-gdelt selected
      YAML schema pasted into inline editor
      Agent re-prepared: DRAFT → PREPARED

8.  Write test_agent.py (boto3 end-to-end test)
      Installed boto3 inside .venv
      invoke_agent() via bedrock-agent-runtime client

9.  Run end-to-end test
      .venv/Scripts/python.exe test_agent.py
      Query: "Find insurgency events in West Africa"
      Result: 0 events (valid), agent suggested refinements per Rule 1

10. Commit and push to GitHub
      action_group_schema.json, local_test.py, test_payload.json,
      .gitignore, test_agent.py pushed to main
```

##### Architecture Clarification (Critical)
Correct data flow — Bedrock is ALWAYS the entry point, not Claude:
```
Analyst → Bedrock → Claude → Bedrock → Lambda → Bedrock → Claude → Analyst
```
- Bedrock receives the query, builds complete prompt (system prompt + tool
  definitions from OpenAPI schema + analyst query), sends to Claude
- Claude never receives raw analyst input directly
- Claude never reads the OpenAPI schema directly — only sees the tool
  definition block Bedrock generated from the schema
- Claude outputs structured text expressing tool-call intention
- Bedrock intercepts the token stream, extracts tool name + parameters,
  invokes Lambda, feeds result back to Claude as a tool result

Two phases:
- Setup phase (once): register OpenAPI schema → Bedrock reads → generates
  tool definitions → stores. Lambda ARN registered. Agent prepared.
- Runtime phase (every query): Bedrock builds full prompt → Claude reasons
  → Claude outputs tool call text → Bedrock intercepts → Lambda invoked →
  result returned to Claude → final response to analyst

##### Step 1 — Lambda Packaging (Linux Binaries)
Lambda runs on Amazon Linux 2 (x86_64). pandas/numpy contain compiled C
extensions that are OS-specific. Normal Windows pip install downloads Windows
DLL files that crash on Lambda with `os.add_dll_directory AttributeError`.

Correct packaging sequence:
```bash
# Install Linux-compatible binaries into package/ folder
pip install pandas requests \
  --platform manylinux2014_x86_64 \
  --target ./package \
  --implementation cp \
  --python-version 3.11 \
  --only-binary=:all:

# Copy source files to package root (Lambda looks for handler at ZIP root)
copy gdelt_query.py ./package/
copy lambda_handler.py ./package/

# ZIP from INSIDE package/ so files land at root level of ZIP
cd ./package
Compress-Archive -Path * -DestinationPath ../deployment.zip
cd ..
```
ZIP size: 47MB (Linux .so binaries are larger than Windows DLLs)
Lambda ZIP limit: 50MB compressed / 250MB uncompressed

Why 512MB memory:
- RAM needed: pandas ~50MB + numpy ~30MB + GDELT ZIP ~20MB + DataFrame ~15MB
  + Python overhead ~30MB = ~145MB minimum (default 128MB → MemoryError crash)
- CPU scales with memory: 512MB → 2x vCPU vs 0.5 vCPU at 128MB
- Vectorised regex across 50,000 GDELT rows is CPU-intensive
- With 128MB: 20-30s execution (risks 60s timeout)
- With 512MB: 5-10s execution (safe)

##### Step 2 — IAM Role Creation
Lambda must have an execution role BEFORE create-function:
```bash
# Create role with trust policy (who can use this role)
aws iam create-role \
  --role-name sentinel-lambda-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"}]}'

# Attach CloudWatch logging permissions (what the role can do)
aws iam attach-role-policy \
  --role-name sentinel-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```
Trust policy = WHO can use the role. Permission policy = WHAT it can do.
Both required. Role ARN: arn:aws:iam::048420889730:role/sentinel-lambda-role

##### Step 3 — Lambda Deployment
```bash
aws lambda create-function \
  --function-name sentinel-signal-monitor-gdelt \
  --runtime python3.11 \
  --role arn:aws:iam::048420889730:role/sentinel-lambda-role \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://deployment.zip \
  --timeout 60 \
  --memory-size 512 \
  --region us-east-1
```
- `--handler lambda_handler.lambda_handler` = filename.functionname (two-part)
- `--runtime python3.11` must exactly match `--python-version 3.11` used in pip
- `fileb://` prefix reads as binary (not base64) from local filesystem
- Timeout 60s: GDELT fetch (5-15s) + CSV parse (5-10s) + filter (1-2s) = ~27s max
- Lambda ARN: arn:aws:lambda:us-east-1:048420889730:function:sentinel-signal-monitor-gdelt

##### Step 4 — Grant Bedrock Permission to Invoke Lambda
Two separate IAM permission directions required:

Direction 1 — Lambda resource policy (Bedrock → Lambda):
```bash
aws lambda add-permission \
  --function-name sentinel-signal-monitor-gdelt \
  --statement-id bedrock-invoke-permission \
  --action lambda:InvokeFunction \
  --principal bedrock.amazonaws.com \
  --source-account 048420889730 \
  --region us-east-1
```
`--source-account` prevents other AWS accounts abusing the permission.
Without this: Bedrock → Lambda access denied silently.

Direction 2 — IAM execution role (Lambda → CloudWatch):
Already done in Step 2 (AWSLambdaBasicExecutionRole).
Without this: Lambda runs but no logs — cannot debug failures.

##### Step 5 — Direct Lambda CLI Test (Before Bedrock)
Always test Lambda in isolation before connecting to Bedrock.
Saved test payload to file — PowerShell inline JSON causes encoding errors:
```json
{
  "actionGroup": "gdelt-query-action",
  "apiPath": "/query-gdelt",
  "httpMethod": "POST",
  "requestBody": {
    "content": {
      "application/json": {
        "properties": [
          {"name": "keywords", "type": "string", "value": "[\"conflict\",\"attack\"]"},
          {"name": "region",   "type": "string", "value": "Africa"}
        ]
      }
    }
  }
}
```
```bash
aws lambda invoke \
  --function-name sentinel-signal-monitor-gdelt \
  --payload fileb://test_payload.json \
  --cli-binary-format raw-in-base64-out \
  response.json \
  --region us-east-1

cat response.json   # always read this — StatusCode 200 ≠ code succeeded
```
Result: StatusCode 200, no FunctionError, structured JSON with correct
keywords, region filter, retrieved_at timestamp confirmed.

##### Step 6 — OpenAPI Schema (action_group_schema.json)
Key design decisions:
- `keywords` is `type: string` NOT `type: array` — Bedrock Action Group
  parameter serialisation is inconsistent with complex types. Define as string,
  Claude generates JSON-encoded string, Lambda parses with json.loads().
- Every description field is an instruction to Claude at inference time, not
  documentation. Example: "JSON array of search keywords as string e.g.
  [\"insurgency\",\"conflict\"]" → Claude generates exactly that format.
- Stored as JSON in Git (action_group_schema.json), pasted as YAML into
  Bedrock inline editor (easier to write without bracket/quote errors).

Validation: Errors 0, Warnings 0 in Bedrock inline editor.

Full schema (action_group_schema.json):
```json
{
  "openapi": "3.0.0",
  "info": {
    "title": "GDELT Query API",
    "version": "1.0.0",
    "description": "Query GDELT for real-time geopolitical conflict events"
  },
  "paths": {
    "/query-gdelt": {
      "post": {
        "summary": "Query GDELT for conflict events",
        "description": "Fetches latest geopolitical conflict events from GDELT matching provided keywords and optional region filter",
        "operationId": "queryGdelt",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["keywords"],
                "properties": {
                  "keywords": {
                    "type": "string",
                    "description": "JSON array of search keywords as string e.g. [\"insurgency\",\"conflict\"]"
                  },
                  "region": {
                    "type": "string",
                    "description": "Optional geographic region filter e.g. West Africa, Middle East"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Successful response with conflict events",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "events": {
                      "type": "array",
                      "description": "List of conflict events matching the query"
                    },
                    "query": {
                      "type": "object",
                      "description": "Query metadata including keywords used and retrieval timestamp"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

Equivalent YAML (pasted into Bedrock inline editor):
```yaml
openapi: 3.0.0
info:
  title: GDELT Query API
  version: 1.0.0
  description: Query GDELT for real-time geopolitical conflict events
paths:
  /query-gdelt:
    post:
      summary: Query GDELT for conflict events
      description: >
        Fetches latest geopolitical conflict events from GDELT matching
        provided keywords and optional region filter
      operationId: queryGdelt
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - keywords
              properties:
                keywords:
                  type: string
                  description: >
                    JSON array of search keywords as string
                    e.g. ["insurgency","conflict"]
                region:
                  type: string
                  description: >
                    Optional geographic region filter
                    e.g. West Africa, Middle East
      responses:
        "200":
          description: Successful response with conflict events
          content:
            application/json:
              schema:
                type: object
                properties:
                  events:
                    type: array
                    description: List of conflict events matching the query
                  query:
                    type: object
                    description: >
                      Query metadata including keywords used
                      and retrieval timestamp
```

##### Step 7 — Action Group Attachment
- Action Group name: gdelt-query-action
- Lambda selected: sentinel-signal-monitor-gdelt
- YAML schema pasted into Bedrock inline editor
- Agent prepared successfully (DRAFT → PREPARED)

##### Step 8 — End-to-End Test (test_agent.py)
```python
client = boto3.client('bedrock-agent-runtime', region_name='us-east-1')
response = client.invoke_agent(
    agentId='OPABTSHSPN',
    agentAliasId='TSTALIASID',
    sessionId='test-session-001',
    inputText='Find insurgency events in West Africa'
)
```
Run via .venv Python interpreter (boto3 installed inside .venv).

Live test result:
- Query: "Find insurgency events in West Africa"
- Lambda returned: events: [], total_found: 0 (valid — current 15-min GDELT
  window had no matching results for that query)
- Agent response: "No insurgency-related events were found in West Africa at
  this time... You may want to try refining your search with more specific
  keywords (e.g., Mali, Nigeria, or Burkina Faso)..."
- Zero events is correct behaviour. Agent 1 correctly interpreted empty result
  and suggested refinements per RULE 1 (clarify before acting).

##### Errors Encountered and Fixed on Day 5
Error 1 — InvalidRequestContentException on Lambda invoke:
  PowerShell inline JSON for --payload caused encoding errors.
  Fix: saved payload to test_payload.json, used fileb://test_payload.json.
  Rule: always use a file for multi-line JSON in PowerShell.

Error 2 — os.add_dll_directory AttributeError:
  pandas installed on Windows included Windows DLL loader code incompatible
  with Lambda's Amazon Linux 2. Crashed on first import.
  Fix: deleted package/ and deployment.zip, reinstalled with
  --platform manylinux2014_x86_64. ZIP grew 39MB → 47MB (Linux .so larger).

Error 3 — FunctionError: Unhandled confusion:
  StatusCode 200 from invoke API ≠ function code succeeded.
  StatusCode = did AWS receive the invoke call.
  FunctionError = did Python code crash inside the function.
  Fix: always cat response.json after invoke — actual result is always there.

##### Architectural Decision — LangGraph over Bedrock Supervisor (Week 3)
Bedrock Supervisor rejected:
  Black box routing. No visibility into orchestration decisions.
  Cannot inspect why routing decisions are made. Cannot debug wrong routing.

LangGraph chosen for Week 3:
  Explicit Python TypedDict state. Every routing edge is Python code.
  LangGraph Studio visual debugging.
  AWS officially endorses LangGraph + Bedrock combination (April 2025 blog).
  LangGraph nodes call Bedrock agents via boto3 — same boto3 calls already known.

Impact on existing work: NONE.
  Bedrock Agents, Lambda, Action Groups, Knowledge Bases are all
  orchestration-layer agnostic. Only change in Week 3: Bedrock Supervisor
  not created, Step Functions replaced with LangGraph graph.

##### Key References
- Lambda ARN: arn:aws:lambda:us-east-1:048420889730:function:sentinel-signal-monitor-gdelt
- Lambda function version: $LATEST
- Action Group name: gdelt-query-action
- Agent ID: OPABTSHSPN (sentinel-signal-monitor)
- Lambda IAM role: arn:aws:iam::048420889730:role/sentinel-lambda-role

##### Day 5 Git Commits
- "Day 5 complete: test_agent.py added — boto3 invoke_agent end-to-end verified."
  Files: action_group_schema.json, local_test.py, test_payload.json, .gitignore,
         test_agent.py pushed to main

---

#### DAY 6 — Confidence Scoring + LangGraph Foundations (IN PROGRESS)

##### Day 6 Execution Sequence (In Order)
```
PART 1 — Fix Agent 1: Add Confidence Scoring to Lambda   ✓ COMPLETE
  1.  Normalised gdelt_query.py output — actors, location, region lowercase
  2.  Removed duplicate format_date() definition
  3.  Added calculate_confidence() to gdelt_query.py
  4.  Wired confidence_score into run_query() return dict (all paths)
  5.  Repackaged and redeployed Lambda (update-function-code)
  6.  Updated Agent 1 instruction prompt — added confidence_score to
      output schema, added 2-3 sentence prose summary before JSON block
  7.  Fixed test_agent.py — uuid4() session ID, removed hardcoded ID
  8.  Deleted duplicate test_agent.py from nested wrong path
  9.  Diagnosed West Africa 0-event root cause — vocabulary mismatch
      (GDELT stores "Nigeria" not "West Africa" — region filter fix)

PART 2 — LangGraph Foundations                           ✓ COMPLETE
  10. LangGraph v1.0.10 installed in root .venv
  11. Created orchestration/__init__.py
  12. Written orchestration/state.py — SentinelState TypedDict
  13. Fixed .vscode/settings.json — root .venv interpreter + extraPaths

PART 3 — Wire Agent 1 into LangGraph                     ✓ COMPLETE
  14. Written orchestration/pipeline.py:
      - signal_monitor_node()
      - route_after_signal()
      - build_pipeline() + compiled pipeline at module level
  15. Fixed indentation errors in pipeline.py
  16. Fixed Pylance import errors (.vscode/settings.json)

REMAINING
  17. Write orchestration/test_pipeline.py — run graph.invoke() end-to-end
  18. Commit all orchestration/ files to main
```

##### Part 1 — calculate_confidence() (gdelt_query.py)

Scoring logic — 4 components, additive:
```
0.1   base score   — always present, even on 0 events (pipeline never gets zero)
+0.3  event base   — at least 1 event returned (resets base to 0.4 total)
+0.2  volume       — 5 or more events (pattern, not noise)
+0.15 recency      — most recent event within 24 hours
+0.25 goldstein    — average Goldstein <= -4 (consistent destabilising signal)

Min: 0.1  Max: 1.0  (0.1 + 0.3 + 0.2 + 0.15 + 0.25 = 1.0)
```

`confidence_score` added to all return paths in `run_query()`:
- Success path: `calculate_confidence(events)` called after query
- Error paths (Timeout, HTTPError, Exception): `confidence_score: 0.1`

Output casing normalised in same session:
- `actors_involved`: `str(a).lower()` — was raw GDELT uppercase
- `event_description` actor/location fields: `.lower()`
- `region_filter`: `region.lower() if region else None`
- Duplicate `format_date()` definition removed

Lambda redeployment sequence:
```bash
rm -rf ./package && rm -f deployment.zip
pip install pandas requests --platform manylinux2014_x86_64 --target ./package --implementation cp --python-version 3.11 --only-binary=:all:
cp gdelt_query.py ./package/ && cp lambda_handler.py ./package/
cd ./package && powershell -Command "Compress-Archive -Path * -DestinationPath ../deployment.zip -Force"
cd .. && aws lambda update-function-code --function-name sentinel-signal-monitor-gdelt --zip-file fileb://deployment.zip --region us-east-1
```

Agent 1 instruction prompt updated:
- OUTPUT FORMAT section: agent now returns 2-3 sentence prose summary
  before the JSON block (analyst-facing context before machine data)
- `confidence_score` added to JSON output schema so agent passes it through

Vocabulary mismatch diagnosis:
- "West Africa" returns 0 events — GDELT stores specific countries/cities,
  never regional groupings. `str.contains('west africa')` matches nothing.
- Fix: use specific country names ("Nigeria", "Mali"). Week 2 Knowledge Base
  will handle regional concept mapping via semantic search.

##### Part 2 — SentinelState TypedDict (orchestration/state.py)

```python
from typing import TypedDict, List

class SentinelState(TypedDict):

    # Pipeline-level — set once, never changed
    session_id:        str       # uuid4 per pipeline run
    analyst_query:     str       # raw analyst question
    retrieved_at:      str       # ISO timestamp set at pipeline start

    # Agent 1 — Signal Monitor
    gdelt_events:      List[dict]  # list of event dicts from GDELT
    signal_confidence: float       # 0.1–1.0 from calculate_confidence()
    signal_summary:    str         # full raw text response from Agent 1

    # Pipeline control
    abort:             bool        # True if pipeline should stop early
    abort_reason:      str         # human-readable reason for abort

    # Error tracking
    errors:            List[str]   # accumulates errors from all nodes
```

Why TypedDict: LangGraph requires TypedDict — reads field annotations to
manage state merging. Each node returns only the fields it changes;
LangGraph merges the rest. No runtime enforcement needed.

##### Part 3 — LangGraph Pipeline (orchestration/pipeline.py)

All three components (node, router, graph) consolidated into one file.

Data flow:
```
analyst_query (from state)
     │
     ▼
signal_monitor_node
     │  calls invoke_agent(OPABTSHSPN / TSTALIASID)
     │  collects streaming chunks → raw string
     │  brace-counting JSON extraction (handles prose prefix)
     │  returns: gdelt_events, signal_confidence, signal_summary, errors
     │
route_after_signal(state)
     │
     ├── signal_confidence >= 0.4  →  'context_historian'  (END stub)
     └── signal_confidence <  0.4  →  'clarification'      (END stub)
```

Key implementation detail — JSON extraction uses brace counting:
```python
json_start = raw.find('{')
brace_count = 0
for i in range(json_start, len(raw)):
    if raw[i] == '{': brace_count += 1
    elif raw[i] == '}': brace_count -= 1
    if brace_count == 0:
        json_end = i + 1
        break
json_block = raw[json_start:json_end]
```
Reason: Agent 1 now returns prose summary before the JSON block.
`rfind('}')` would break if prose contains `}` characters.
Brace counting finds the outermost matching `{}` pair reliably.

Routing threshold: `>= 0.4` → context_historian, else → clarification.
Both currently terminate at END (stubs — Week 2 wires context_historian).

Graph assembly:
```python
graph = StateGraph(SentinelState)
graph.add_node('signal_monitor', signal_monitor_node)
graph.set_entry_point('signal_monitor')
graph.add_conditional_edges('signal_monitor', route_after_signal,
    {'context_historian': END, 'clarification': END})
pipeline = graph.compile()   # compiled at module level on import
```

##### Files Built on Day 6
```
agents/signal_monitor/gdelt_query.py       MODIFIED — calculate_confidence(),
                                            casing normalisation, dedup
agents/signal_monitor/test_agent.py        MODIFIED — uuid4 session ID
agents/signal_monitor/instruction_prompt.txt MODIFIED — confidence_score in
                                            schema, prose summary rule
orchestration/__init__.py                  NEW
orchestration/state.py                     NEW — SentinelState TypedDict
orchestration/pipeline.py                  NEW — node + router + graph
```

##### Remaining for Day 6 Exit Criterion
```
orchestration/test_pipeline.py             NOT YET WRITTEN
  — graph.invoke() end-to-end test
  — confirm signal_confidence populated
  — confirm route decision printed
  — commit all orchestration/ files to main
```

---

#### DAY 7 — Context Historian: Seed Knowledge Graph (IN PROGRESS)

##### What Was Built
File: `agents/context_historian/graph/seed_graph.py`
File: `agents/context_historian/graph/knowledge_graph.pkl`

A hand-coded NetworkX directed graph (`nx.DiGraph`) covering the Sahel/West
Africa conflict zone from 2003 to present. Serialised to `.pkl` for fast
loading by Agent 2 at query time.

Folder cleanup: duplicate `seed_graph.py` created at nested path
`agents/context_historian/agents/context_historian/graph/seed_graph.py`
was deleted. Correct path: `agents/context_historian/graph/seed_graph.py`.

##### Graph Structure

5 node types, 7 relationship (edge) types.

**Node Types:**

| Type | Count | Description |
|------|-------|-------------|
| Region | 22 | Countries and sub-regions (Mali, Niger, Kidal, Liptako_Gourma, etc.) |
| Actor | 15 | Armed groups and state forces (JNIM, ISGS, AQIM, Wagner, FAMA, etc.) |
| ExternalActor | 16 | External states and institutions (France, Russia, UN, ECOWAS, etc.) |
| Event | 16 | Key historical events (2012 Mali Coup, 2013 Serval, 2017 JNIM Formation, etc.) |
| Conflict | 10 | Named conflict threads (Sahel_Insurgency, Liptako_Gourma_Crisis, etc.) |

**Edge/Relationship Types:**

| Relationship | Count | Direction | Example |
|-------------|-------|-----------|---------|
| borders | 19 | Region → Region | Mali → Niger |
| operates_in | 22 | Actor → Region | JNIM → Northern_Mali |
| affiliated_with | 9 | Actor → Actor | JNIM → AQIM (parent) |
| intervenes_in | 19 | ExternalActor → Region | France → Mali |
| supports | 11 | ExternalActor → Actor/Junta | Russia → Wagner |
| caused_by | 15 | Event → Event | 2023_Wagner_Deployment → 2022_France_Withdrawal |
| part_of | 10 | Conflict → Conflict | Liptako_Gourma_Crisis → Sahel_Insurgency |

**Timeline coverage:** 2003 (GSPC kidnappings) → 2024 (Wagner Niger expansion)

##### Key Design Decisions

Directed graph (`nx.DiGraph`) not undirected:
  Direction encodes meaning. `operates_in` goes Actor → Region, not both ways.
  `caused_by` goes Effect → Cause (causal chain traversal reads naturally).
  `part_of` goes Sub-conflict → Parent-conflict (hierarchical traversal).

Rich node attributes for each node:
  `type`, `founded`/`since`, `status` (active/dissolved/merged/recurring),
  `ideology`, `note`. All queryable by Agent 2 at runtime.

Rich edge attributes for each edge:
  `relationship`, `since`/`start`/`end`, `note`, `mechanism` (for caused_by).
  Enables temporal queries: "what was active in 2015?", "who operated in
  Kidal before the 2022 coup?"

Algeria appears twice (as Region AND ExternalActor):
  Legitimate dual role — Algeria is both a geographic neighbour and an active
  diplomatic actor in the Mali peace process.

Serialised as `.pkl` (pickle):
  NetworkX graph → pickle is the standard persistence format.
  Fast load at Agent 2 query time — no rebuild needed.
  `.pkl` file added to `.gitignore` (binary, rebuilt from seed_graph.py).

##### Notable Entities in Graph

Conflict chain (caused_by edges):
```
2003_GSPC_Kidnappings
  → 2007_AQIM_Founding
      → 2015_ISGS_Founded
      → 2017_JNIM_Formation
2011_Libya_Collapse
  → 2011_MNLA_Founded
      → 2012_Tuareg_Rebellion
          → 2012_Mali_Coup
          → 2013_Operation_Serval
              → 2015_Operation_Barkhane
                  → 2017_G5_Sahel_Force
                  → 2022_France_Withdrawal (caused by 2021 coup)
                      → 2023_Wagner_Deployment
```

Liptako_Gourma — highest priority node:
  Tri-border zone Mali-Niger-Burkina Faso. JNIM and ISGS both operate_in it.
  G5_Sahel_Force operates_in it (degraded). Part of Sahel_Insurgency.
  "Highest attack density globally" (node note attribute).

##### Functions
```python
build_seed_graph() -> nx.DiGraph   # constructs and returns the full graph
save_graph(G, path)                # pickles G to .pkl file, prints node/edge count
load_graph(path) -> nx.DiGraph     # loads and returns graph from .pkl

# Entry point:
if __name__ == '__main__':
    G = build_seed_graph()
    save_graph(G)                  # → knowledge_graph.pkl
```

##### Test Results
- `build_seed_graph()` executed without error
- `save_graph()` confirmed: nodes and edges counted and printed
- `knowledge_graph.pkl` created at `agents/context_historian/graph/`
- Graph traversal tested — node/edge structure verified

##### Files
```
agents/context_historian/graph/seed_graph.py       NEW — graph definition
agents/context_historian/graph/knowledge_graph.pkl NEW — serialised graph (not in git)
```

---

#### Day 8 — Bedrock Knowledge Base + Pinecone Setup — COMPLETE (2026-03-10)

##### What was built
- [x] IAM service role created: `AmazonBedrockExecutionRoleForKnowledgeBase_sentinel`
      ARN: `arn:aws:iam::048420889730:role/AmazonBedrockExecutionRoleForKnowledgeBase_sentinel`
      Trust policy: `bedrock.amazonaws.com` can assume role
      Inline policy `BedrockKBPermissions` grants:
        - `s3:GetObject` / `s3:ListBucket` on `sentinel-osint-knowledge-base`
        - `bedrock:InvokeModel` (for embedding calls)
        - `secretsmanager:GetSecretValue` on `sentinel/pinecone-api-key*`

- [x] Pinecone API key stored in AWS Secrets Manager
      Secret name: `sentinel/pinecone-api-key`
      Secret ARN: `arn:aws:secretsmanager:us-east-1:048420889730:secret:sentinel/pinecone-api-key-3w4haJ`
      Format: `{"apiKey": "<key>"}`

- [x] Bedrock Knowledge Base created
      KB ID: `WGLUOKITSP`
      KB ARN: `arn:aws:bedrock:us-east-1:048420889730:knowledge-base/WGLUOKITSP`
      KB name: `sentinel-context-historian-kb`
      Embedding model: `amazon.titan-embed-text-v2:0` (1024 dims)
      Vector store: Pinecone serverless
        Host: `https://sentinel-osint-index-qr67ju4.svc.aped-4627-b74a.pinecone.io`
        Namespace: `geopolitical-history`
        Field mapping: text → `text`, metadata → `metadata`

- [x] S3 data source attached to KB — recreated twice due to parser constraints
      Final Data Source ID: `SOMSV1H4GQ`
      Data Source name: `sentinel-kb-s3-source`
      Bucket: `s3://sentinel-osint-knowledge-base/documents/`
      Parser: Amazon Nova Lite (`amazon.nova-lite-v1:0`) FM parser
      Status: `AVAILABLE`

      Parser selection journey:
        - Default parser: failed — all 4 SIPRI/ACLED PDFs are scanned images, 0 text extracted
        - Claude 3 Haiku: not enabled — needs AWS Marketplace subscription
        - Claude Sonnet 4.6 inference profile: rejected — KB FM parser requires foundation model ARN
        - Claude 3.5 Sonnet / Claude 3 Sonnet: inference-profile-only / deprecated
        - Amazon Titan Text Premier: end-of-life
        - Amazon Nova Lite: ACCESSIBLE, accepted as FM parser — used this
        - Note: `vectorIngestionConfiguration.parsingConfiguration` cannot be updated
          after creation — must delete and recreate data source to change parser

      KB IAM role policy updated to add:
        `bedrock:GetInferenceProfile`, `bedrock:InvokeModelWithResponseStream`

- [x] Pinecone index pre-created by user
      Index name: `sentinel-osint-index`
      Dimensions: 1024
      Metric: cosine
      Cloud: AWS us-east-1 (serverless)

- [x] Documents uploaded to S3 and ingested
      5 PDFs uploaded to `s3://sentinel-osint-knowledge-base/documents/`
        - ACLED_conflict_2026.pdf (4 MB, scanned)
        - codesria_sahel_2011.pdf (169 KB, has text layer)
        - sipri_sahel_2020_june.pdf (3 MB, scanned)
        - sipri_sahel_2021.pdf (5 MB, scanned)
        - sipri_sahel_mali_2020.pdf (3 MB, scanned)
      codesria_sahel_2011.txt also extracted locally via PyMuPDF and uploaded
      Ingestion job `K9UET5WSWS`: scanned=6, indexed=6, failed=0 — COMPLETE
      All 6 documents embedded in Pinecone `geopolitical-history` namespace

##### Architecture note
Documents in `s3://sentinel-osint-knowledge-base/documents/` are parsed by
Amazon Nova Lite (FM parser reads scanned image PDFs page-by-page), chunked,
embedded by `amazon.titan-embed-text-v2:0` (1024 dims), and stored in Pinecone
under the `geopolitical-history` namespace. The Context Historian Bedrock Agent
will be linked to this KB to enable RAG retrieval of historical geopolitical parallels.

##### Next steps (Day 9)
- Create Context Historian Bedrock Agent in Bedrock console
- Write instruction prompt for Context Historian
- Attach KB `WGLUOKITSP` to the agent
- Write action group schema for context retrieval
- Test agent with a sample query against the KB
