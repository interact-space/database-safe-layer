# DB Safe Layer

**A deterministic safety layer for untrusted SQL (LLMs, agents, tools).**

You don’t review SQL. You **own execution**.

---

## What happens before SQL touches your database

### 1. Intercept destructive SQL
_Untrusted SQL is parsed and dry-run **before execution**._

![Risk interception](./db_safe_layer/docs/media/risk-interception.gif)

---

### 2. Gate high-risk operations with explicit approval
_Large or dangerous writes are **blocked or gated**, not silently executed._

![Gate approval](./db_safe_layer/docs/media/gate-approval.gif)

---

### 3. Record evidence and enable deterministic rollback
_Every decision is logged. Approved writes can be **rolled back via snapshot reference**._

![Audit and rollback](./db_safe_layer/docs/media/audit-rollback.gif)

---

### Example: what gets blocked

```bash
safe-layer "DELETE FROM users;"

Risk: CRITICAL
Decision: BLOCKED
```
> This is the default behavior. Full-table destructive SQL never executes silently.

## Why this exists

Automated SQL fails in predictable ways:

- UPDATE / DELETE without WHERE  
- Large destructive writes  
- Schema changes in the wrong environment  
- No reproducible audit or rollback  

Execution — not generation — is the real risk.

---

## Quick Start
### Installation
Clone project
```bash

git clone https://github.com/interact-space/database-safe-layer.git
cd database-safe-layer

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env   # then edit database config

```

Python API
```python

from db_safe_layer import safe_exec, rollback_to

SQL = "DELETE FROM visits WHERE visit_date < '2010-01-01';"

# check SQL and execute only if allowed by policy
result = safe_exec(SQL)

# interactive rollback (lists snapshots and prompts for selection)
rollback_to()

```

CLI 
```bash

# check SQL and execute only if allowed
safe-layer "DELETE FROM visits WHERE visit_date < '2010-01-01';"

# interactive rollback
safe-db-rollback

```

The rollback command will:

1. List available snapshot IDs
2. Prompt the user to select one
3. Restore database state to the selected snapshot

Rollback does **not** re-run the original SQL.
The operation itself is also recorded in the audit log.


High-risk operations (large writes, schema changes) will prompt for explicit approval before execution.

## Design notes

- Execution decisions are deterministic and replayable
- Audit logs are structured and machine-readable
- Rollback restores state without re-running SQL

See code for execution pipeline and audit schema.


## Structure
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

## License
MIT


💬 Join the Discussion

If you have any ideas, suggestions, or questions while using this project, feel free to open an Issue and share your thoughts!
Whether it’s a feature request, bug report, improvement proposal, or general discussion, we truly welcome your participation.

👉 Start the conversation here: [Issues](https://github.com/interact-space/database-safe-layer/issues)
Your feedback helps make this project better — thank you for your support! 🙌

