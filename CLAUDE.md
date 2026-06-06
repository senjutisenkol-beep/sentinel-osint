# Sentinel-OSINT — Claude Code Instructions

## Project
Multi-Agent Geopolitical Risk Detection System on AWS Bedrock.
4-week build plan. Goal: build AND learn engineering + scientific complexity.

## AWS
- Region: us-east-1 | Account: 048420889730
- IAM User: Senjutisen | Profile: senjutisen (configured in ~/.aws)

## Model
claude-sonnet-4-6 (`anthropic.claude-sonnet-4-6-20251001-v1:0`)
Do not use Claude 3.5 Sonnet or any other model version.

## Build Rules
- Do not write any agent code, Lambda functions, schemas, or infrastructure until explicitly instructed
- Follow the 4-week plan in bedrock-mas-roadmap.html exactly
- Build one week at a time, one day-block at a time

## Environment
- Python 3.14 (Windows) | Shell: bash via Claude Code (Unix syntax)
- Installed: boto3, botocore, python-dotenv, pytest, langgraph, langsmith

## Key Resource IDs
| Resource | ID / ARN |
|----------|----------|
| Agent 1 | OPABTSHSPN / Alias: TSTALIASID |
| Lambda | arn:aws:lambda:us-east-1:048420889730:function:sentinel-signal-monitor-gdelt |
| Lambda role | arn:aws:iam::048420889730:role/sentinel-lambda-role |
| KB | WGLUOKITSP (sentinel-context-historian-kb) |
| KB data source | SOMSV1H4GQ |
| Pinecone | sentinel-osint-index-qr67ju4.svc.aped-4627-b74a.pinecone.io / ns: geopolitical-history |
| S3 | s3://sentinel-osint-knowledge-base/ |
| ReliefWeb appname | senjutisen-geoint-research-x7k2NBxz56heKW29d |

## Architectural Decisions
- Agent 1 returns ALL Goldstein signs — Agent 3 applies asymmetric weighting (negative full weight, positive 0.5x)
- Context Historian = pure Python + LangGraph, not a Bedrock Agent — `invoke_model` not `invoke_agent`
- Lambda imports MCP server `query_all_sources()` directly (not subprocess — Lambda is single-invocation)
- Routing threshold: `>= 0.1` → context_historian (0.1 = base/no-events, not failure)
- Vector score threshold: 0.50 — lower to 0.40 after KB grows past ~20 docs
- MCP sources: GDELT (live 15-min) + NewsAPI (7-day) + ReliefWeb v2 (UN OCHA)
- KB parser: Amazon Nova Lite (scanned PDFs) — cannot update parser after creation, must recreate data source
- LangSmith EU endpoint: `https://eu.api.smith.langchain.com`

## Lambda Redeploy (reference)
```bash
rm -rf ./package && rm -f deployment.zip
pip install <deps> --platform manylinux2014_x86_64 --target ./package \
  --implementation cp --python-version 3.11 --only-binary=:all:
cp agents/signal_monitor/lambda_handler.py agents/mcp_server/*.py ./package/
cd ./package && Compress-Archive -Path * -DestinationPath ../deployment.zip -Force
cd .. && aws lambda update-function-code --function-name sentinel-signal-monitor-gdelt \
  --zip-file fileb://deployment.zip --region us-east-1
```
Always use `--only-binary=:all:` — pydantic has a compiled Rust extension that breaks on Linux if built on Windows.

## Progress Summary
Full day-by-day notes: `docs/progress/week1-2.md`

| Day | Status | What was built |
|-----|--------|----------------|
| 1–2 | COMPLETE | AWS setup, repo, shared utils |
| 3 | COMPLETE | Bedrock Agent (OPABTSHSPN), model access via Marketplace |
| 4 | COMPLETE | gdelt_query.py, lambda_handler.py, local test |
| 5 | COMPLETE | Lambda deployed, Action Group, end-to-end test |
| 6 | COMPLETE | Confidence scoring, LangGraph, SentinelState |
| 7 | COMPLETE | NetworkX knowledge graph (79 nodes, Sahel 2003–2024) |
| 8 | COMPLETE | Bedrock KB + Pinecone, 3-channel retrieval, synthesiser |
| 9 | COMPLETE | LangSmith tracing, bottleneck: Agent 1 = 68% of 41s total |
| 10 | COMPLETE | Threshold calibration: routing 0.1, vector 0.50 |
| 11 | COMPLETE | MCP server live (3 sources), Lambda upgraded, Agent 3 state fields added |

## Week 3 Plan

### Status
- [x] Day 11 — MCP server live, Lambda upgraded, Agent 3 state fields added to state.py
- [ ] Day 12 — Agent 3: Threat Analyst
- [ ] Day 13 — Agent 4: Red Team
- [ ] Day 14 — Step Functions Express Workflow
- [ ] Day 15 — Flash Report + end-to-end test

### Pre-Week 3 Open Issues
- [ ] Vector threshold 0.50 — confirm chunks returned after KB enrichment
- [ ] Niger query Agent 1 reproducible empty response (Step Functions retry will handle)
- [ ] Synthesiser JSON parse error on out-of-scope queries — add robust extraction

### Target Architecture
```
Input → Step Functions
  ├── PARALLEL: Agent 1 (MCP Lambda) + graph_retrieve + temporal_retrieve
  └── merge → Agent 2 (synthesiser) → Agent 3 → CHOICE (HIGH?) → Agent 4 → Flash Report → S3
```
Latency target: <30s. Current: ~41s. Parallel branch saves ~15s.

### Agent 3 — Threat Analyst (Day 12)
`invoke_model` directly (no tool calls). SentinelState fields already added (Day 11).
```json
{"threat_level": "HIGH|MEDIUM|LOW|UNKNOWN", "threat_score": 0.0–1.0,
 "threat_rationale": "...", "key_indicators": [...], "threat_confidence": "HIGH|MEDIUM|LOW"}
```
Scoring: negative Goldstein → full weight increase | positive → 0.5x weight decrease
signal_confidence scales overall score | context_confidence weights Agent 2 contribution

### Agent 4 — Red Team (Day 13)
Runs only when `threat_level == HIGH`. Adds 3 fields to SentinelState:
`counter_evidence: List[str]`, `revised_threat_score: float`, `red_team_summary: str`
```json
{"counter_evidence": [...], "revised_threat_score": 0.0–1.0,
 "revised_confidence": 0.0–1.0, "challenge_summary": "..."}
```

### Step Functions (Day 14)
Express Workflow. Parallel state: Agent 1 + graph/temporal retrieval (independent — run concurrently).
Provisioned concurrency on Lambda eliminates 3–5s cold start.
Built-in retry → fixes Niger empty response without code changes.

### Flash Report (Day 15)
Saved to `s3://sentinel-osint-knowledge-base/reports/<uuid>.json`
Fields: report_id, generated_at, analyst_query, threat_level, threat_score, executive_summary,
signal_summary, historical_context, threat_rationale, counter_evidence, revised_threat_score,
gdelt_events, confidence_scores {signal, context, threat}, data_gaps

### Week 3 Exit Criteria
- [ ] KB enrichment automated — weekly EventBridge cron → S3 → Bedrock sync
- [ ] All 4 agents wired into Step Functions state machine
- [ ] Flash Report JSON produced for 3 test queries
- [ ] Step Functions visual graph committed to README
- [ ] Total pipeline latency < 30s
- [ ] Niger query Agent 1 error resolved
