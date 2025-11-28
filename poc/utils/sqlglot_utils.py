import sqlglot
from sqlglot import parse_one, exp

def get_statement_type(sql: str) -> str:
    """获取 SQL 语句类型"""
    try:
        node = parse_one(sql)
        return node.key.upper()
    except Exception:
        return "UNKNOWN"

def get_tables(sql: str) -> list:
    """获取 SQL 语句中涉及的表名"""
    try:
        node = parse_one(sql)
        return [t.name for t in node.find_all(exp.Table)]
    except Exception:
        return []

def is_read_only(sql: str) -> bool:
    """判断 SQL 是否为只读操作"""
    try:
        node = parse_one(sql)
        return isinstance(node, exp.Select)
    except Exception:
        # 如果解析失败，保守地认为不是只读
        return False
    
def wrap_count_subquery(sql: str) -> str:
    try:
        # 1. 解析 SQL 为抽象语法树 (AST)
        expression = sqlglot.parse_one(sql)

        # 2. 性能优化：移除 ORDER BY
        # 在统计总数时，排序是完全浪费资源的操作。
        # 我们直接修改 AST，把 'order' 部分抹去。
        if isinstance(expression, exp.Select):
            expression.set("order", None)

        # 3. 构建新的 COUNT 查询
        # 使用 sqlglot 的构建器模式：
        # SELECT COUNT(*) AS estimated_rows FROM ( <原查询> ) AS t
        return (
            sqlglot.select("COUNT(*) AS estimated_rows")
            .from_(expression.subquery("t"))
            .sql()
        )
    except Exception as e:
        # 如果解析失败（例如 SQL 语法极其错误），回退到简单的字符串拼接
        # 或者选择抛出异常
        print(f"解析优化失败，回退到普通拼接: {e}")
        return f"SELECT COUNT(*) AS estimated_rows FROM ({sql}) t"

def pretty(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, read="duckdb", write="PostgreSQL")[0]
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