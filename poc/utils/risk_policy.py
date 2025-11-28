from .sqlglot_utils import get_statement_type, get_tables
from sqlglot import exp
from typing import Dict, Any

def assess_risk(sql: str, estimated_rows: int | None = None):
    st = get_statement_type(sql)
    risk = "low"
    needs_approval = False

    if st in ("DELETE", "UPDATE", "ALTER", "DROP", "TRUNCATE", "CREATE"):
        risk = "high"
        needs_approval = True
    elif st == "SELECT":
        risk = "low"
    else:
        risk = "medium"

    if estimated_rows is not None:
        if estimated_rows > 10000:
            # 简单规则：行数大提升风险
            risk = "medium" if risk == "low" else risk

    return {
        "statement_type": st,
        "tables": get_tables(sql),
        "risk": risk,
        "needs_approval": needs_approval
    }

def analyze_risk(expression: exp.Expression) -> Dict[str, Any]:
    """
    分析 SQL 风险等级（扩展版）
    支持 DDL / DML / DCL / TCL / UTILITY
    """

    # ===========================
    #   DDL（最高风险）
    # ===========================
    if isinstance(expression, exp.Drop):
        level = "CRITICAL"
        sql_type = "Drop"
        reason = "DROP 删除结构对象"

    # elif isinstance(expression, exp.Truncate):
    #     level = "CRITICAL"
    #     sql_type = "Truncate"
    #     reason = "TRUNCATE 删除全表数据"

    elif isinstance(expression, exp.Alter):
        level = "HIGH"
        sql_type = "Alter"
        reason = "ALTER 修改数据库结构"       

    elif isinstance(expression, (exp.Create, exp.Schema)):
        level = "HIGH"
        sql_type = "Create"
        reason = "CREATE 操作可能修改结构"

    # ===========================
    #   DML
    # ===========================

    # SELECT：安全
    elif isinstance(expression, exp.Select):
        level = "LOW"
        sql_type = "Select"
        reason = "只读查询 (SELECT)"

    # INSERT
    elif isinstance(expression, exp.Insert):
        level = "MEDIUM"
        sql_type = "Insert"
        reason = "INSERT 写入数据"

    # UPDATE
    elif isinstance(expression, exp.Update):
        if not expression.find(exp.Where):
            level = "HIGH"
            sql_type = "Update"
            reason = "UPDATE 无 WHERE → 可能全表更新"
        else:
            level = "MEDIUM"
            sql_type = "Update"
            reason = "UPDATE 修改数据"

    # DELETE
    elif isinstance(expression, exp.Delete):
        if not expression.find(exp.Where):
            level = "HIGH"
            sql_type = "Delete"
            reason = "DELETE 无 WHERE → 全表删除"
        else:
            level = "MEDIUM"
            sql_type = "Delete"
            reason = "DELETE 删除数据"

    # MERGE
    elif isinstance(expression, exp.Merge):
        level = "HIGH"
        sql_type = "Merge"
        reason = "MERGE 高风险写操作"

    # ===========================
    #   DCL（权限）
    # ===========================
    elif isinstance(expression, exp.Grant):
        level = "HIGH"
        sql_type = "Grant"
        reason = "GRANT 修改权限"

    elif isinstance(expression, exp.Revoke):
        level = "HIGH"
        sql_type = "Revoke"
        reason = "REVOKE 回收权限"

    # ===========================
    #   TCL（事务控制）
    # ===========================
    elif isinstance(expression, exp.Commit):
        level =  "INFO"
        sql_type = "Commit"
        reason = "事务提交"

    elif isinstance(expression, exp.Rollback):
        level = "INFO"
        sql_type = "Rollback"
        reason = "事务回滚"

    # ===========================
    #   Utility 语句
    # ===========================
    # elif isinstance(expression, exp.Explain):
    #     level = "INFO"
    #     sql_type = "Explain"
    #     reason = "EXPLAIN 查询分析"

    elif isinstance(expression, exp.Analyze):
        level = "MEDIUM"
        sql_type = "Analyze"
        reason = "ANALYZE 收集统计信息"

    # elif hasattr(exp, "Vacuum") and isinstance(expression, exp.Vacuum):
    #     level = "HIGH"
    #     sql_type = "VACUUM"
    #     reason = "VACUUM 影响数据库性能"

    elif isinstance(expression, exp.Comment):
        level = "LOW"
        sql_type = "Comment"
        reason = "注释语句"

    # ===========================
    #   未分类的语句
    # ===========================
    else:
        level = "unknown"
        sql_type = "unknown"
        reason = f"无法识别的 SQL 类型：{expression.__class__.__name__}"
    return {
        "risk_level": level,
        "sql_type" : sql_type,
        "reason": reason
    }
if __name__ == "__main__":
    print(exp.Truncate)