import sqlglot
from sqlglot import exp
from typing import Dict, Any

def pretty(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, read="duckdb", write="PostgreSQL")[0]
    except Exception:
        return sql

def extract_sql_details(expression: exp.Expression) ->Dict[str, Any]:
    """
    Extract key information from sqlglot expressions
    
    Args:
        expression: object returned by sqlglot.parse_one()
        
    Returns:
        {sql_type, table_names, where_clause}
        -sql_type: SQL type (SELECT, UPDATE, DELETE...)
        -table_names: the collection of table names involved (remove duplication)
        -where_clause: string form of WHERE clause (None if none)
    """
    
    # 1. Get SQL type
    # expression.key usually returns 'select', 'insert', etc., converted to uppercase
    sql_type = expression.key.upper()
    
    # 2. Get the involved tables (Tables)
    # find_all(exp.Table) will recursively find all tables, including tables in JOIN and subqueries
    tables = set()
    for table in expression.find_all(exp.Table):
        # table.name Gets only the table name (e.g. 'users')
        # table.sql() will get the full path (e.g. 'public.users')
        # Here we use table.name, you can also change it to table.sql( according to your needs)
        tables.add(table.name)
        
   # 3. Get WHERE condition
    # We give priority to obtaining the WHERE of the current level (mainly for DELETE/UPDATE/SELECT)
    # Use args.get("where") to avoid accidentally obtaining the WHERE in the subquery
    where_node = expression.args.get("where")
    
    where_clause = None
    if where_node:
       # Convert AST nodes back to SQL strings for easy reading
        where_clause = where_node.sql()
        
    return {
        "sql_type": sql_type,
        "tables" : list(tables),
        "where_clause": where_clause
    }

# ==========================================
#test code
# ==========================================

if __name__ == "__main__":
   # Test 1: Simple SELECT
    sql1 = "SELECT * FROM users WHERE age > 18"
    expr1 = sqlglot.parse_one(sql1)
    print(f"SQL1: {extract_sql_details(expr1)}")
   # Expected: ('SELECT', {'users'}, 'WHERE age > 18')

    # Test 2: Dangerous DELETE (with conditions)
    sql2 = "DELETE FROM orders WHERE status = 'cancelled' AND date < '2023-01-01'"
    expr2 = sqlglot.parse_one(sql2)
    print(f"SQL2: {extract_sql_details(expr2)}")
   # 预期: ('DELETE', {'orders'}, "WHERE status = 'cancelled' AND date < '2023-01-01'")

    # Test 3: Extremely dangerous UPDATE (unconditional)
    sql3 = "UPDATE person SET active = 0"
    expr3 = sqlglot.parse_one(sql3)
    print(f"SQL3: {extract_sql_details(expr3)}")
    # Expected: ('UPDATE', {'person'}, None)

   # Test 4: Complex JOIN
    sql4 = "SELECT t1.name, t2.salary FROM employee t1 JOIN salary t2 ON t1.id = t2.emp_id WHERE t1.active = 1"
    expr4 = sqlglot.parse_one(sql4)
    print(f"SQL4: {extract_sql_details(expr4)}")
   # 预期: ('SELECT', {'employee', 'salary'}, 'WHERE t1.active = 1')