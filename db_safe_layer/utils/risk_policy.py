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
            # Simple rule: a large number of lines increases the risk
            risk = "medium" if risk == "low" else risk

    return {
        "statement_type": st,
        "tables": get_tables(sql),
        "risk": risk,
        "needs_approval": needs_approval
    }

def analyze_risk(expression: exp.Expression, estimated_rows: int | None = None) -> Dict[str, Any]:
    """
    Analyze SQL Risk Levels (Extended Edition)
    Support DDL/DML/DCL/TCL/UTILITY
    """

    # ===========================
    #  Ddl (highest risk)
    # ===========================
    if isinstance(expression, exp.Drop):
        level = "CRITICAL"
        sql_type = "Drop"
        reason = "DROP 删除结构对象"

    # elif isinstance(expression, exp.Truncate):
    #     level = "CRITICAL"
    #     sql_type = "Truncate"
    #     reason = "TRUNCATE deletes the entire table data"

    elif isinstance(expression, exp.Alter):
        level = "HIGH"
        sql_type = "Alter"
        reason = "ALTER modifies the database structure"       

    elif isinstance(expression, (exp.Create, exp.Schema)):
        level = "HIGH"
        sql_type = "Create"
        reason = "CREATE operations may modify structures"

    # ===========================
    #   DML
    # ===========================

    # SELECT: SECURITY
    elif isinstance(expression, exp.Select):
        level = "LOW"
        sql_type = "Select"
        reason = "Read-only query (SELECT)"

    # INSERT
    elif isinstance(expression, exp.Insert):
        level = "MEDIUM"
        sql_type = "Insert"
        reason = "INSERT writes data"

    # UPDATE
    elif isinstance(expression, exp.Update):
        if not expression.find(exp.Where):
            level = "HIGH"
            sql_type = "Update"
            reason = "UPDATE without WHERE → may update the entire table"
        else:
            level = "MEDIUM"
            sql_type = "Update"
            reason = "UPDATE modifies data"

    # DELETE
    elif isinstance(expression, exp.Delete):
        if not expression.find(exp.Where):
            level = "HIGH"
            sql_type = "Delete"
            reason = "DELETE None WHERE → delete the entire table"
        else:
            level = "MEDIUM"
            sql_type = "Delete"
            reason = "DELETE delete data"

    # MERGE
    elif isinstance(expression, exp.Merge):
        level = "HIGH"
        sql_type = "Merge"
        reason = "MERGE high-risk write operation"

    # ===========================
    #   Dcl (permission)
    # ===========================
    elif isinstance(expression, exp.Grant):
        level = "HIGH"
        sql_type = "Grant"
        reason = "GRANT modify permissions"

    elif isinstance(expression, exp.Revoke):
        level = "HIGH"
        sql_type = "Revoke"
        reason = "REVOKE reclaim permissions"

    # ===========================
    # Tcl (transaction control)
    # ===========================
    elif isinstance(expression, exp.Commit):
        level =  "INFO"
        sql_type = "Commit"
        reason = "Transaction Commit"

    elif isinstance(expression, exp.Rollback):
        level = "INFO"
        sql_type = "Rollback"
        reason = "Transaction rollback"

    # ===========================
    #  Utility statement
    # ===========================
    # elif isinstance(expression, exp.Explain):
    #     level = "INFO"
    #     sql_type = "Explain"
    #     reason = "EXPLAIN query analysis"

    elif isinstance(expression, exp.Analyze):
        level = "MEDIUM"
        sql_type = "Analyze"
        reason = "ANALYZE collect statistics"

    # elif hasattr(exp, "Vacuum") and isinstance(expression, exp.Vacuum):
    #     level = "HIGH"
    #     sql_type = "VACUUM"
    #     reason = "VACUUM affects database performance"

    elif isinstance(expression, exp.Comment):
        level = "LOW"
        sql_type = "Comment"
        reason = "comment statement"

    # ===========================
    #   Uncategorized sentences
    # ===========================
    else:
        level = "unknown"
        sql_type = "unknown"
        reason = f"Unrecognized SQL type：{expression.__class__.__name__}"
    
    if estimated_rows is not None:
        if estimated_rows > 10000:
            # Simple rule: a large number of lines increases the risk
            level = "medium" if level == "low" else level
    
    return {
        "risk_level": level,
        "sql_type" : sql_type,
        "reason": reason
    }
if __name__ == "__main__":
    print(exp.Truncate)