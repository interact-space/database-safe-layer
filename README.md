# DB Safe Layer
## AI-generated SQL must pass through a Safe Execution Layer before execution.
Database safety is no longer a human review task.<br>
LLM agents, automation scripts, and internal tools can generate SQL—but **execution remains the real risk**.

DB Safe Layer is a lightweight SQL firewall that intercepts every statement before it reaches your database and performs:<br>
•	SQL structural analysis<br>
•	Dry-run impact estimation<br>
•	Risk classification<br>
•	Optional snapshot creation<br>
•	Gated execution<br>
•	Full audit + deterministic replay<br>

Just drop it in front of your database—no infra changes.

### “Like having a tireless DBA reviewing every command.”

## Why this is needed

Teams across analytics, data engineering, healthcare, finance, and consulting report the same problem:<br>
	•	LLMs sometimes generate hallucinated or destructive SQL<br>
	•	Developers rely on manual review (slow + error-prone)<br>
	•	Operations lack audit logs and replayability<br>
	•	Even staging databases get damaged accidentally<br>

DB Safe Layer provides a deterministic safety boundary before SQL touches any real data.

### Execution Pipeline (Deterministic)

```text
User SQL Input
        │
        ▼
   precheck(sql)
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
                                        │
                                        ▼
                                write audit.json
                                        │
                                        ▼
                                return result

```
Every step is recorded.<br> 
Every run can be replayed deterministically.


### Features

#### 1. Structural Risk Analysis

Using SQLGlot AST parsing—not regex.<br>
Detects:<br>
	•	DROP / TRUNCATE / ALTER<br>
	•	DELETE / UPDATE without WHERE<br>
	•	Cross-table mutations<br>
	•	Write operations on protected tables<br>

Produces standardized risk levels: **LOW / MEDIUM / HIGH / CRITICAL**.

#### 2. Dry-Run (Non-Destructive Impact Estimation)

Before running a write query:
```text
DELETE → SELECT COUNT(*)
UPDATE → SELECT COUNT(*)
INSERT → SELECT COUNT(*) FROM VALUES(...)
```
Allows users to see:

##### “This will update 3,214 rows. Proceed?”

#### 3. Automatic Snapshot

For high-risk operations, DB Safe Layer creates a snapshot:<br>
	•	SQLite → file copy<br>
	•	PostgreSQL → CREATE TABLE AS snapshot / txid<br>

Snapshots are references, and backups—fast and reversible.

#### 4. Full Audit + Replay

Every run logs:<br>
	•	SQL<br>
	•	Parsed AST<br>
	•	Risk level<br>
	•	Dry-run result<br>
	•	Snapshot reference<br>
	•	Execution decision<br>
	•	Final result<br>

Replay re-executes only read-only steps, without touching the database.



Structure
```text

db-safe-layer/
│
├── app.py                 # Example runner
├── db/
│   ├── config.py          # DB configuration
│   ├── database.py        # Engine/session management
│   └── snapshot.py        # Snapshot creation
│
├── execution/
│   └── executor.py        # Precheck → Dry-run → Risk → Execution
│
├── utils/
│   ├── risk_policy.py     # Risk classifier
│   └── sqlglot_helper.py  # SQL AST parsing + rewriting
│
├── audit/
│   ├── log_manager.py     # Write audit logs
│   └── replay.py          # Deterministic replay
│
└── requirements.txt
```
### SQL → Precheck →  Dry-Run → Risk analyse  → Approval → Snapshot → Execution DAG → Audit + Replay 

### Stack
- SQLGlot：SQL AST、dry-run（SELECT COUNT(*) FROM (...)
- SQLAlchemy 


## Quick Start
### Installation
Clone project
```
Bash

git clone https://github.com/interact-space/database-safe-layer.git
```
Install
```
Bash

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```
Configure
Copy .env.example -> .env and configure the database connection:

## Quick Start
✅ Python API
```
python

from db_safe_ayer import safe_exec, rollback_to
#module SQL check
safe_exec(SQL)
#module rollback to specified snapshot id
rollback_to()
```

✅ CLI 
```
bash
#SQL check
safe-layer <SQL>
#rollback to specified snapshot id
safe-db-rollback 
```

💬 Join the Discussion

If you have any ideas, suggestions, or questions while using this project, feel free to open an Issue and share your thoughts!
Whether it’s a feature request, bug report, improvement proposal, or general discussion, we truly welcome your participation.

👉 Start the conversation here: [Issues](https://github.com/interact-space/database-safe-layer/issues)
Your feedback helps make this project better — thank you for your support! 🙌




 
