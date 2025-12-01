import sqlglot
from sqlglot import parse_one, exp

def get_statement_type(sql: str) -> str:
    """Get SQL statement type"""
    try:
        node = parse_one(sql)
        return node.key.upper()
    except Exception:
        return "UNKNOWN"

def get_tables(sql: str) -> list:
    """Get the table names involved in the SQL statement"""
    try:
        node = parse_one(sql)
        return [t.name for t in node.find_all(exp.Table)]
    except Exception:
        return []

def is_read_only(sql: str) -> bool:
    """Determine whether SQL is a read-only operation"""
    try:
        node = parse_one(sql)
        return isinstance(node, exp.Select)
    except Exception:
        # If parsing fails, it is conservatively considered not read-only.
        return False
    
def wrap_count_subquery(sql: str) -> str:
    try:
        #1. Parse SQL into abstract syntax tree (AST)
        expression = sqlglot.parse_one(sql)

        # 2. Performance optimization: remove ORDER BY
        # When counting totals, sorting is a completely wasteful operation.
        # We directly modify the AST and delete the 'order' part.
        if isinstance(expression, exp.Select):
            expression.set("order", None)

       # 3. Build a new COUNT query
        # Use sqlglot's builder pattern:
        # SELECT COUNT(*) AS estimated_rows FROM ( <原查询> ) AS t
        return (
            sqlglot.select("COUNT(*) AS estimated_rows")
            .from_(expression.subquery("t"))
            .sql()
        )
    except Exception as e:
       # If parsing fails (for example, the SQL syntax is extremely wrong), fall back to simple string concatenation
        # Or choose to throw an exception
        print(f"Parsing optimization failed and fell back to normal splicing: {e}")
        return f"SELECT COUNT(*) AS estimated_rows FROM ({sql}) t"

def pretty(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, read="duckdb", write="PostgreSQL")[0]
    except Exception:
        return sql
def get_sql_operation_type(sql_code):
    """
    Parse SQL and return operation type (CRUD /DDL) and specific command
    """
    try:
        # parse may return a list of multiple statements, so we iterate over it
        parsed_expressions = sqlglot.parse(sql_code)
    except Exception as e:
        return [{"sql": sql_code, "type": "ERROR", "detail": str(e)}]

    results = []

    for expression in parsed_expressions:
        # Get the original SQL fragment (for display)
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
            # Further determine whether to create a table, view or index
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
            
        # ---3. Others (DCL /TCL) ---
        elif isinstance(expression, exp.Grant) or isinstance(expression, exp.Revoke):
            op_category = "DCL (Permissions)"
            op_detail = "GRANT/REVOKE"
        elif isinstance(expression, exp.Commit) or isinstance(expression, exp.Rollback):
            op_category = "TCL (Transaction)"
            op_detail = "TRANSACTION CONTROL"

       # Extract the main table name involved (simple extraction logic)
        tables = [t.name for t in expression.find_all(exp.Table)]
        
        results.append({
            "category": op_category,
            "detail": op_detail,
            "tables_involved": tables,
            "sql_preview": raw_sql[:50] + "..." if len(raw_sql) > 50 else raw_sql
        })

    return results