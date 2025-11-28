# SQL → Dry-Run → Risk analyse → Approval → Snapshot → Execution DAG → Audit + Replay  ( LangGraph + SQLGlot)

## What is this
User SQL Input
        │
        ▼
   dry_run(sql)
        │
        ▼
 estimated_rows
        │
        ▼
 analyze_risk(sql, rows)
        │
        ├── LOW → execute(sql)
        │
        └── MEDIUM/HIGH → ask user yes/no
                        │
                        ├── no → abort
                        │
                        └── yes → snapshot() → execute(sql)
        
        ▼
 write audit.json
        ▼
   return result


## Stack
- LangGraph：编排 / DAG
- SQLGlot：SQL AST、dry-run（SELECT COUNT(*) FROM (...))、风险要素分析
- SQLAlchemy + SQLite：最小 DB（person / condition_occurrence）
- OpenAI 兼容接口：可走 Ollama（本地）或 OpenAI（云端）
- 审计：JSON 文件（runs/）

## Quick Start
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# 如用 Ollama（本地）：
# LLM_MODE=local, LLM_BASE_URL=http://127.0.0.1:11434/v1, LLM_API_KEY=ollama, LLM_MODEL=llama3:latest

python -m poc.app
 