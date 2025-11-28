from .log_manager import load_run
from poc.execution.executor import run_sql
from poc.utils.sqlglot_utils import is_read_only

def replay(run_id: str):
    """
    回放功能：重放之前的 SQL 执行
    对于只读操作，可以安全重放
    对于非只读操作，会被阻止
    """
    run = load_run(run_id)
    sql = run.get("sql")
    steps = run.get("execution_dag", [])
    re_results = []
    
    # 如果没有直接的 SQL，尝试从步骤中提取
    if not sql:
        for s in steps:
            if s.get("action") == "execute_sql":
                sql = s.get("inputs", {}).get("sql")
                break
    
    if not sql:
        return {"run_id": run_id, "status": "error", "error": "No SQL found in run record"}
    
    # 检查是否为只读操作
    if not is_read_only(sql):
        return {
            "run_id": run_id,
            "status": "blocked",
            "message": "Non-read-only operations cannot be replayed for safety reasons",
            "sql": sql
        }
    
    # 重放只读操作
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