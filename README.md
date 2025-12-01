# 🛡️ DB Safe Layer (SQL Firewall)
DB Safe Layer is an intelligent database operation security middleware. It is located between the application and the database and is responsible for in-depth syntax analysis, risk assessment, impact estimation (Dry Run) and automatic snapshot backup before executing SQL to prevent data disasters caused by human errors.

"Like having a tireless DBA reviewing every command sent to the database."

📂 Structure
```text

db-safe-layer/
├──
│   ├── app.py                  # Entry test script
│   ├── db/
│   │   ├── config.py           # Database configuration
│   │   ├── database.py         # SQLAlchemy engine management
│   │   └── snapshot.py         # Snapshot and rollback logic (core)
│   ├── execution/
│   │   └── executor.py         # Safety Actuator (Integrated Risk Analysis with DryRun)
│   ├── utils/
│   │   ├── risk_policy.py      # Risk Level Analyzer
│   │   └── sqlglot_helper.py   # sqlglot method
│   ├── audit/
│   │   ├── log_manager.py      # Record audit documents
│   │   └── replay.py           # Replays a previous SQL execution.
│    
├── .env                        # environment variables
└── requirements.txt            # Dependency package
```
## SQL → Precheck →  Dry-Run → Risk analyse  → Approval → Snapshot → Execution DAG → Audit + Replay  

### What is this

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


### Stack
- SQLGlot：SQL AST、dry-run（SELECT COUNT(*) FROM (...)
- SQLAlchemy 

## ✨ Core Features (Features)
### 🧠 Intelligent risk analysis (Risk Analysis)

Perform AST (Abstract Syntax Tree) analysis based on sqlglot instead of simple regular matching.
Accurately identify high-risk operations such as DROP and UPDATE/DELETE without WHERE conditions.
Automatic rating: LOW, MEDIUM, HIGH, CRITICAL.

### 🔮 Pre-execution deduction (Dry Run)

Read operation: directly estimate the result set size.

Write operation: Intelligent conversion of DELETE/UPDATE into SELECT COUNT(*), without modifying the data, informs the user how many rows of data will be affected.

### 📸 Automatic Snapshot & Rollback

Pre-emptive backup: Automatically create table-level snapshots for high-risk operations (supports SQLite file replication & PostgreSQL CREATE TABLE AS).

One-click rollback: Provides Time Machine function to support data recovery to any historical snapshot point.

Automatic adaptation: The code automatically detects the underlying database dialect and adapts to CASCADE (Postgres) or PRAGMA (SQLite).

### 📝 Full link audit (Audit)

Record the SQL, risk level, user decision, execution time and snapshot ID of each operation.

Supports operation replay (Replay) to facilitate problem reproduction.



### 🛠️Installation
Clone project
```
Bash

git clone https://github.com/interact-space/database-safe-layer.git
```

Create a virtual environment and install dependencies
```
Bash

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```
Configure environment variables Copy .env.example to .env and configure the database connection:

## Quick Start
- Test DB Safe Layer
You can modify the input sql in app.py (SQL1 --> SQL10)
```
python -m db_safe_layer.app
```

- Test rollback
```
python -m db_safe_layer.db.snapshot
```





 