import sqlglot
from sqlglot import parse_one, exp

READ_ONLY_TYPES = {"SELECT"}

def get_statement_type(sql: str) -> str:
    try:
        node = parse_one(sql)
        return node.key.upper()
    except Exception:
        return "UNKNOWN"

def is_read_only(sql: str) -> bool:
    t = get_statement_type(sql)
    return t in READ_ONLY_TYPES

def wrap_count_subquery(sql: str) -> str:
    # SELECT COUNT(*) FROM ( <sql> ) t
    return f"SELECT COUNT(*) AS estimated_rows FROM ({sql}) t"

def get_tables(sql: str):
    try:
        node = parse_one(sql)
        return [t.name for t in node.find_all(exp.Table)]
    except Exception:
        return []

def pretty(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, read="duckdb", write="sqlite")[0]
    except Exception:
        return sql
def get_sql_operation_type(sql_code):
    """
    解析 SQL 并返回操作类型 (CRUD / DDL) 以及具体的命令
    """
    try:
        # parse 可能会返回多条语句的列表，所以我们遍历它
        parsed_expressions = sqlglot.parse(sql_code)
    except Exception as e:
        return [{"sql": sql_code, "type": "ERROR", "detail": str(e)}]

    results = []

    for expression in parsed_expressions:
        # 获取原始 SQL 片段（用于展示）
        raw_sql = expression.sql()
        
        op_category = "UNKNOWN"
        op_detail = "UNKNOWN"

        # --- 1. CRUD (DML - Data Manipulation Language) ---
        if isinstance(expression, exp.Select):
            op_category = "CRUD - READ"
            op_detail = "SELECT (Query)"
        elif isinstance(expression, exp.Insert):
            op_category = "CRUD - CREATE"
            op_detail = "INSERT (Add Data)"
        elif isinstance(expression, exp.Update):
            op_category = "CRUD - UPDATE"
            op_detail = "UPDATE (Modify Data)"
        elif isinstance(expression, exp.Delete):
            op_category = "CRUD - DELETE"
            op_detail = "DELETE (Remove Data)"
        elif isinstance(expression, exp.Merge):
            op_category = "CRUD - COMPLEX"
            op_detail = "MERGE (Upsert)"
            
        # --- 2. DDL (Data Definition Language) ---
        elif isinstance(expression, exp.Create):
            op_category = "DDL - CREATE"
            # 进一步判断是创建表、视图还是索引
            kind = expression.args.get("kind", "OBJECT")
            op_detail = f"CREATE {kind.upper()}"
        elif isinstance(expression, exp.Drop):
            op_category = "DDL - DROP"
            kind = expression.args.get("kind", "OBJECT")
            op_detail = f"DROP {kind.upper()}"
        elif isinstance(expression, exp.Alter):
            op_category = "DDL - ALTER"
            op_detail = "ALTER (Modify Structure)"
        elif isinstance(expression, exp.Truncate):
            op_category = "DDL - TRUNCATE"
            op_detail = "TRUNCATE (Clear Table)"
            
        # --- 3. 其他 (DCL / TCL) ---
        elif isinstance(expression, exp.Grant) or isinstance(expression, exp.Revoke):
            op_category = "DCL (Permissions)"
            op_detail = "GRANT/REVOKE"
        elif isinstance(expression, exp.Commit) or isinstance(expression, exp.Rollback):
            op_category = "TCL (Transaction)"
            op_detail = "TRANSACTION CONTROL"

        # 提取涉及的主表名（简单的提取逻辑）
        tables = [t.name for t in expression.find_all(exp.Table)]
        
        results.append({
            "category": op_category,
            "detail": op_detail,
            "tables_involved": tables,
            "sql_preview": raw_sql[:50] + "..." if len(raw_sql) > 50 else raw_sql
        })

    return results