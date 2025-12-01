from .log_manager import load_run
from db_safe_layer.execution.executor import run_sql
from db_safe_layer.utils.sqlglot_utils import is_read_only

def replay(run_id: str):
    """
    Replay Functionality: Replays a previous SQL execution.
    Read-only operations can be safely replayed.
    Non-read-only operations will be blocked.
    """
    run = load_run(run_id)
    sql = run.get("sql")
    steps = run.get("execution_dag", [])
    re_results = []
    
    # If no direct SQL is available, attempt to extract it from the steps.
    if not sql:
        for s in steps:
            if s.get("action") == "execute_sql":
                sql = s.get("inputs", {}).get("sql")
                break
    
    if not sql:
        return {"run_id": run_id, "status": "error", "error": "No SQL found in run record"}
    
    # Check if it is a read-only operation.
    if not is_read_only(sql):
        return {
            "run_id": run_id,
            "status": "blocked",
            "message": "Non-read-only operations cannot be replayed for safety reasons",
            "sql": sql
        }
    
   # Replay the read-only operation.
    try:
        res = run_sql(sql)
        return {
            "run_id": run_id,
            "status": "replayed",
            "sql": sql,
            "result": res,
            "result_count": len(res) if res else 0
        }
    except Exception as e:
        return {
            "run_id": run_id,
            "status": "error",
            "error": str(e),
            "sql": sql
        }