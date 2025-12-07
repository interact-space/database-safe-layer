import datetime, json
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from db_safe_layer.utils.sqlglot_helper import extract_sql_details, pretty
# from db_safe_layer.utils.sqlglot_utils import  wrap_count_subquery, pretty, get_statement_type, get_tables
from db_safe_layer.utils.risk_policy import  analyze_risk
from db_safe_layer.db.database import DatabaseManager
from db_safe_layer.db.config import settings
import sqlglot
from sqlglot import exp

def rewrite_to_count(sql: str) -> str:
    """
    Black magic function: Convert any DML (Update/Delete/Insert) to SELECT COUNT(*)
    """
    try:
        expression = sqlglot.parse_one(sql)
        
        # -------------------------------------------------------
        # 1. Process SELECT /WITH /UNION (maintain original logic)
        # -------------------------------------------------------
        if isinstance(expression, exp.Select) or isinstance(expression, exp.Union):
            # Remove ORDER BY (optimize performance)
            if isinstance(expression, exp.Select):
                expression.set("order", None)
            return sqlglot.select("COUNT(*) AS estimated_rows").from_(expression.subquery("t")).sql()

        # -------------------------------------------------------
        # 2. Handle DELETE and UPDATE
        # Logic: extract table name + extract WHERE condition -> assemble into SELECT COUNT(*)
        # -------------------------------------------------------
        if isinstance(expression, (exp.Delete, exp.Update)):
           # Find the target table
            # Note: In the Update/Delete structure of sqlglot, table is usually in this or find(exp.Table)
            target_table = expression.find(exp.Table)
            if not target_table:
                return None
            
            # Find WHERE clause
            where_clause = expression.args.get("where")
            
            # Build new query
            count_query = sqlglot.select("COUNT(*) AS estimated_rows").from_(target_table)
            
            # If there is a WHERE condition, add it; if not, it is COUNT of the entire table
            if where_clause:
                count_query = count_query.where(where_clause)
                
            return count_query.sql()

        # -------------------------------------------------------
       # 3. Process INSERT
        # -------------------------------------------------------
        if isinstance(expression, exp.Insert):
            # Case A: INSERT INTO ... VALUES (...)
            # This does not require checking the database, just count the number of elements in values directly
            if isinstance(expression.expression, exp.Values):
                values_node = expression.expression
                # This is a Value list, directly returns the SQL of the length of the list (simulation)
                # Or calculate it directly at the Python layer. Here, in order to uniformly return SQL strings
                row_count = len(values_node.expressions)
               # Construct a SELECT that does not require table lookup
                return f"SELECT {row_count} AS estimated_rows"

          # Case B: INSERT INTO ... SELECT ...
            # This requires running the subsequent SELECT
            if isinstance(expression.this, exp.Select):
                source_query = expression.this
                return sqlglot.select("COUNT(*) AS estimated_rows").from_(source_query.subquery("t")).sql()

        # -------------------------------------------------------
        #4. Handling TRUNCATE (DDL)
        # -------------------------------------------------------
        # if isinstance(expression, exp.Truncate):
        #     # Truncate clears the entire table, so we count the number of rows in the entire table
        #      target_table = expression.this
        #      if target_table:
        #          return sqlglot.select("COUNT(*) AS estimated_rows").from_(target_table).sql()

    except Exception as e:
        print(f"⚠️ Dry Run SQL conversion failed: {e}")
        return None
    
    return None

    
def run_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Real execution of SQL (parameterized security version)"""
    # ⚠️ Please make sure to replace this with your actual DB connection code
    db = DatabaseManager(settings.DB_URL, echo=False)
    with db.session() as s:
       # Use parameterized queries to prevent injection
        rs = s.execute(text(sql), params or {})
        
        # If it is INSERT/UPDATE/DELETE, there may be no returns, handle this situation
        if rs.returns_rows:
            cols = rs.keys()
            rows = rs.fetchall()
            return [dict(zip(cols, r)) for r in rows]
        else:
            # For write operations, return the number of affected rows as the result
            return [{"affected_rows": rs.rowcount}]
        
def run_dry_estimate(sql: str):
    """
    Intelligent estimation of row number (supports SELECT, UPDATE, DELETE, INSERT)
    """
    # 1. Try to convert the SQL into a count query
    count_sql = rewrite_to_count(sql)
    
    if not count_sql:
       # If conversion cannot be performed (such as complex stored procedure calls), return -1
        return -1, None
    
    print(f"   [DryRun] Generated Count SQL: {count_sql}")
    
   # 2. Execute count query
    try:
       # Special handling: if it is static INSERT VALUES, count_sql may be "SELECT 5 AS estimated_rows"
        # This does not require complicated from, and can be run directly with run_sql (depending on the database supporting SELECT without FROM, such as Postgres/SQLite support, Oracle requires FROM DUAL）
        
        result = run_sql(count_sql)
        if result and len(result) > 0:
           # Compatible with different keys and return (count, count(*), estimated_rows)
            # Our rewrite function is forced to use the alias AS estimated_rows
            val = result[0].get("estimated_rows")
            if val is not None:
                return int(val), count_sql
    except Exception as e:
        print(f"   [DryRun] Execution failed: {e}")
    
    return -1, count_sql
def cli_user_confirmation(report: List) -> bool:
    """User confirmation function to securely access report data"""
    print("\n" + "="*60)
    print("⚠️  High risk operation warning")
    print("="*60)
    
   # Securely obtain risk information
    risk_info = {}
    if len(report) > 0 and "outputs" in report[0]:
        risk_info = report[2].get("outputs", {})
        sql_preview = report[0].get("inputs", {}).get("sql", "N/A")
        print(f"SQL statement: {sql_preview}")
        print(f"Risk level: {risk_info.get('risk_level', 'UNKNOWN')}")
        print(f"reason: {risk_info.get('reason', 'N/A')}")
        print(f"Operation type: {risk_info.get('sql_type', 'UNKNOWN')}")
    
   # Safely get the estimated number of rows
    estimated_rows = -1
    if len(report) > 1 and "outputs" in report[1]:
        estimated_rows = report[1].get("outputs", {}).get("estimated_rows", -1)
    
    if estimated_rows >= 0:
        print(f"Estimated number of affected rows: {estimated_rows}")
    else:
        print("Estimated number of affected rows: Unable to estimate")
    
    print("="*60)
    
    while True:
        choice = input("\nContinue execution?(yes/no): ").strip().lower()
        if choice in ("yes", "y"):
            return True
        elif choice in ("no", "n"):
            return False
        else:
            print("Please enter yes or no")


def execute_sql_with_safety(raw_sql: str) -> Dict[str, Any]:
    """
    New security enforcement process:
    ① risk_level = analyze_risk(sql, estimated_rows)
    ② dry_run(): only estimate affected rows, not executed
    ③ If risk = LOW → execute SQL directly
    ④ If risk = MEDIUM /HIGH → Print prompt → Wait for user yes/no
    ⑤ User yes → Create snapshot (automatic transaction or temporary backup)
    ⑥Execute SQL
    ⑦ Write audit.json (processed by the caller)
    ⑧ Provide replay function (rollback or replay)
    
    Args:
        sql: SQL statement to be executed
        auto_confirm: whether to confirm automatically (for testing or scripts)
    
    Returns:
        Dictionary containing execution results, risk information, snapshot ID, etc.
    """
    audit_steps = []
    snapshot_id = None
    result = None
    risk_level = "UNKNOWN"
    risk_info = {}
    
    try:
        expression = sqlglot.parse_one(raw_sql)
    except Exception as e:
        # If SQL parsing fails, use default value
        expression = None
        risk_info = {
            "risk_level": "UNKNOWN",
            "sql_type": "UNKNOWN",
            "reason": f"SQL Parsing failed: {str(e)}"
        }
        risk_level = "UNKNOWN"

   # ① precheck: parse SQL type 
    precheck_record = {
        "step_id": "step 1",
        "action": "precheck",
        "start_at": datetime.datetime.utcnow().isoformat(),
        "inputs": {"sql": pretty(raw_sql)},
        "outputs": {},
        "status": "pending"
    }
    try:
        if expression:
            sql_details = extract_sql_details(expression)
            precheck_record["outputs"]["operation_type"] = sql_details.get("sql_type", "UNKNOWN")
            precheck_record["outputs"]["tables"] = sql_details.get("tables", "UNKNOWN")
            precheck_record["outputs"]["predicate"] = sql_details.get("where_clause", "UNKNOWN") 
        precheck_record["status"] = "success"
    except Exception as e:
        precheck_record["status"] = "error"
        precheck_record["error"] = str(e)

    finally:
        precheck_record["end_at"] = datetime.datetime.utcnow().isoformat()
        audit_steps.append(precheck_record)


    # ② dry_run: estimate the number of affected rows and rewrite
    dry_run_record = {
        "step_id": "step 2",
        "action": "dry_run",
        "start_at": datetime.datetime.utcnow().isoformat(),
        "inputs": {"sql": pretty(raw_sql)},
        "outputs": {},
        "status": "pending"
    }
    try:
        estimated_rows, dry_run_sql = run_dry_estimate(raw_sql)
        dry_run_record["outputs"]["estimated_rows"] = estimated_rows
        dry_run_record["outputs"]["dry_run_sql"] = dry_run_sql

        dry_run_record["status"] = "success"
    except Exception as e:
        dry_run_record["status"] = "error"
        dry_run_record["error"] = str(e)
        estimated_rows = -1
    finally:
        dry_run_record["end_at"] = datetime.datetime.utcnow().isoformat()
        audit_steps.append(dry_run_record)
    

    # ③ analyze_risk :  low-, medium-, or high-risk.

    analyze_risk_record = {
        "step_id": "step 3",
        "action": "analyze_risk",
        "start_at": datetime.datetime.utcnow().isoformat(),
        "inputs": {"sql": pretty(raw_sql), "estimated_rows": estimated_rows},
        "outputs": {},
        "status": "pending"
    }

    try:
        if expression:
            risk_info = analyze_risk(expression, estimated_rows)
            risk_level = risk_info.get("risk_level", "UNKNOWN")
        analyze_risk_record["outputs"] = risk_info
        analyze_risk_record["status"] = "success"
    except Exception as e:
        analyze_risk_record["status"] = "error"
        analyze_risk_record["error"] = str(e)
       # Set default value
        if not risk_info:
            risk_info = {
                "risk_level": "UNKNOWN",
                "sql_type": "UNKNOWN",
                "reason": f"Risk analysis failed: {str(e)}"
            }
            risk_level = "UNKNOWN"
    finally:
        analyze_risk_record["end_at"] = datetime.datetime.utcnow().isoformat()
        audit_steps.append(analyze_risk_record)
    
    
    
   # ③ Decide whether to execute based on the risk level
    if risk_level in ("LOW", "INFO"):
       # LOW and INFO risks are executed directly
        execute_record = {
            "step_id": "step 4",
            "action": "execute_sql",
            "start_at": datetime.datetime.utcnow().isoformat(),
            "inputs": {"sql": pretty(raw_sql)},
            "outputs": {},
            "status": "pending"
        }
        try:
            result = run_sql(raw_sql)
            execute_record["outputs"]["result"] = result
            execute_record["outputs"]["result_count"] = len(result) if result else 0
            execute_record["status"] = "success"
            print(f"✅ SQL execution successful")
        except Exception as e:
            execute_record["status"] = "error"
            execute_record["error"] = str(e)
            print(f"❌ SQL execution failed: {str(e)}")
        finally:
            execute_record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(execute_record)
    elif risk_level == "UNKNOWN" or risk_level == "unknown":
        #Unknown type, execution refused
        execute_record = {
            "step_id": "step 4",
            "action": "execute_sql",
            "start_at": datetime.datetime.utcnow().isoformat(),
            "inputs": {"sql": pretty(raw_sql)},
            "outputs": {},
            "status": "error"
        }
        execute_record["error"] = "Unrecognized SQL type, execution refused"
        execute_record["end_at"] = datetime.datetime.utcnow().isoformat()
        audit_steps.append(execute_record)
        print(f"❌Unrecognized SQL type, execution refused")
    else:
       # MEDIUM, HIGH, CRITICAL require user confirmation
        user_confirmed = cli_user_confirmation(audit_steps)

        if not user_confirmed:
            confirmation_record = {
                "step_id": "step 4",
                "action": "User_confirmation",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "user_choice": "No",
                "status": "cancelled"
            }
            confirmation_record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(confirmation_record)
            print("❌ User cancels execution")
        else:
            confirmation_record = {
                "step_id": "step 4",
                "action": "User_confirmation",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "user_choice": "Yes",
                "status": "success"
            }
            confirmation_record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(confirmation_record)

            snapshot_record = {
                "step_id": "step 5",
                "action": "create_snapshot",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "inputs": {},
                "outputs": {},
                "status": "pending"
            }
            try:
                from db_safe_layer.utils.snapshot_manager import create_snapshot_for_operation
                sql_type = risk_info.get("sql_type", "UNKNOWN")
                snapshot_meta = create_snapshot_for_operation(
                    operation_type=sql_type,
                    sql=raw_sql
                )
                # create_snapshot_for_operation returns snapshot_meta dictionary
                if isinstance(snapshot_meta, dict):
                    snapshot_id = snapshot_meta.get("snapshot_id")
                else:
                   # If the returned value is a string (compatible with older versions)
                    snapshot_id = snapshot_meta
                snapshot_record["inputs"] = {"sql": raw_sql}
                snapshot_record["outputs"] = {"snapshot_id": snapshot_id} if snapshot_id else {}
                snapshot_record["status"] = "success"
                if snapshot_id:
                    print(f"✅ Snapshot created: {snapshot_id}")
            except Exception as e:
                snapshot_record["status"] = "error"
                snapshot_record["error"] = str(e)
                print(f"⚠️ Warning: Failed to create snapshot: {str(e)}")
            finally:
                snapshot_record["end_at"] = datetime.datetime.utcnow().isoformat()
                audit_steps.append(snapshot_record)
        

            execute_record = {
                "step_id": "step 6",
                "action": "execute_sql",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "inputs": {"sql": pretty(raw_sql)},
                "outputs": {},
                "status": "pending"}
            try:
                result = run_sql(raw_sql)
                execute_record["outputs"]["result"] = result
                execute_record["outputs"]["result_count"] = len(result) if result else 0
                execute_record["status"] = "success"
                print(f"✅ SQL execution successful")
            except Exception as e:
                execute_record["status"] = "error"
                execute_record["error"] = str(e)
                print(f"❌ SQL execution failed: {str(e)}")
            finally:
                execute_record["end_at"] = datetime.datetime.utcnow().isoformat()
                audit_steps.append(execute_record)
    
    # 生成总结
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d %H:%M:%S")
    operation_type = risk_info.get("sql_type", "UNKNOWN")
    
    if result:
        if len(result) > 0:
            first = result[0]
            if first:
                n = list(first.values())[0] if first else len(result)
                summary = f"{timestamp}，The user executed{operation_type}Operation, return result：{n}"
            else:
                summary = f"{timestamp}，The user performed the {operation_type} operation and returned {len(result)} rows"
        else:
            summary = f"{timestamp}，The user performed the {operation_type} operation and no results were returned."
    else:
        summary = f"{timestamp}，User performed {operation_type} operation"
    
    if snapshot_id:
        summary += f"（Snapshot id: {snapshot_id}）"
    
    
    return {
        "sql": raw_sql,
        "estimated_rows": estimated_rows,
        "risk": risk_level,
        "snapshot_id": snapshot_id,
        "result": result,
        "audit_steps": audit_steps,
        "summary": summary
    }

if __name__ == "__main__":
    print("🚀 Starting SQL Safety Pipeline (LangGraph Framework) ...")
   
    sql = """
    INSERT INTO person (
    person_id,
    gender_concept_id,
    year_of_birth,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value
    )
    VALUES (
        999999,
        8507,
        1985,
        8527,
        38003563,
        999999,
        999999,
        999999,
        'p0003'
    );
    """

    result = execute_sql_with_safety(sql)