import datetime, json
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from poc.utils.sqlglot_utils import  wrap_count_subquery, pretty, get_statement_type, get_tables
from poc.utils.risk_policy import  analyze_risk
from poc.db.database import DatabaseManager
from poc.db.config import settings
import sqlglot
from sqlglot import exp

def rewrite_to_count(sql: str) -> str:
    """
    黑魔法函数：将任意 DML (Update/Delete/Insert) 转换为 SELECT COUNT(*)
    """
    try:
        expression = sqlglot.parse_one(sql)
        
        # -------------------------------------------------------
        # 1. 处理 SELECT / WITH / UNION (保持原有逻辑)
        # -------------------------------------------------------
        if isinstance(expression, exp.Select) or isinstance(expression, exp.Union):
            # 移除 ORDER BY (优化性能)
            if isinstance(expression, exp.Select):
                expression.set("order", None)
            return sqlglot.select("COUNT(*) AS estimated_rows").from_(expression.subquery("t")).sql()

        # -------------------------------------------------------
        # 2. 处理 DELETE 和 UPDATE
        # 逻辑：提取表名 + 提取 WHERE 条件 -> 拼装成 SELECT COUNT(*)
        # -------------------------------------------------------
        if isinstance(expression, (exp.Delete, exp.Update)):
            # 查找目标表
            # 注意：sqlglot 的 Update/Delete 结构中，table 通常在 this 或 find(exp.Table) 中
            target_table = expression.find(exp.Table)
            if not target_table:
                return None
            
            # 查找 WHERE 子句
            where_clause = expression.args.get("where")
            
            # 构建新查询
            count_query = sqlglot.select("COUNT(*) AS estimated_rows").from_(target_table)
            
            # 如果有 WHERE 条件，加进去；如果没有，就是全表 COUNT
            if where_clause:
                count_query = count_query.where(where_clause)
                
            return count_query.sql()

        # -------------------------------------------------------
        # 3. 处理 INSERT
        # -------------------------------------------------------
        if isinstance(expression, exp.Insert):
            # 情况 A: INSERT INTO ... VALUES (...)
            # 这种不需要查库，直接算 values 里的元素个数
            if isinstance(expression.expression, exp.Values):
                values_node = expression.expression
                # 这是一个 Value list，直接返回 list 长度的 SQL (模拟)
                # 或者直接在 Python 层算出来，这里为了统一返回 SQL 字符串
                row_count = len(values_node.expressions)
                # 构造一个不需要查表的 SELECT 
                return f"SELECT {row_count} AS estimated_rows"

            # 情况 B: INSERT INTO ... SELECT ...
            # 这种需要运行后面的 SELECT
            if isinstance(expression.this, exp.Select):
                source_query = expression.this
                return sqlglot.select("COUNT(*) AS estimated_rows").from_(source_query.subquery("t")).sql()

        # -------------------------------------------------------
        # 4. 处理 TRUNCATE (DDL)
        # -------------------------------------------------------
        # if isinstance(expression, exp.Truncate):
        #      # Truncate 是清空全表，所以我们统计全表行数
        #      target_table = expression.this
        #      if target_table:
        #          return sqlglot.select("COUNT(*) AS estimated_rows").from_(target_table).sql()

    except Exception as e:
        print(f"⚠️ Dry Run SQL 转换失败: {e}")
        return None
    
    return None

    
def run_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """真正执行 SQL (参数化安全版)"""
    # ⚠️ 请确保这里替换为你实际的 DB 连接代码
    db = DatabaseManager(settings.DB_URL, echo=False)
    with db.session() as s:
        # 使用参数化查询防止注入
        rs = s.execute(text(sql), params or {})
        
        # 如果是 INSERT/UPDATE/DELETE，可能没有 returns，处理这种情况
        if rs.returns_rows:
            cols = rs.keys()
            rows = rs.fetchall()
            return [dict(zip(cols, r)) for r in rows]
        else:
            # 对于写操作，返回受影响行数作为结果
            return [{"affected_rows": rs.rowcount}]
        
def run_dry_estimate(sql: str):
    """
    智能估算行数 (支持 SELECT, UPDATE, DELETE, INSERT)
    """
    # 1. 尝试将 SQL 转换为计数查询
    count_sql = rewrite_to_count(sql)
    
    if not count_sql:
        # 如果无法转换（比如复杂的存储过程调用），返回 -1
        return -1, None
    
    print(f"   [DryRun] Generated Count SQL: {count_sql}")
    
    # 2. 执行计数查询
    try:
        # 特殊处理：如果是静态 INSERT VALUES，count_sql 可能是 "SELECT 5 AS estimated_rows"
        # 这种不需要复杂的 from，直接 run_sql 也能跑（取决于数据库支持 SELECT without FROM，如 Postgres/SQLite 支持，Oracle 需要 FROM DUAL）
        
        result = run_sql(count_sql)
        if result and len(result) > 0:
            # 兼容不同的 key 返回 (count, count(*), estimated_rows)
            # 我们的 rewrite 函数都强制起了别名 AS estimated_rows
            val = result[0].get("estimated_rows")
            if val is not None:
                return int(val), count_sql
    except Exception as e:
        print(f"   [DryRun] Execution failed: {e}")
    
    return -1, count_sql
def cli_user_confirmation(report: List) -> bool:
    """用户确认函数，安全地访问报告数据"""
    print("\n" + "="*60)
    print("⚠️  高风险操作警告")
    print("="*60)
    
    # 安全地获取风险信息
    risk_info = {}
    if len(report) > 0 and "outputs" in report[0]:
        risk_info = report[0].get("outputs", {})
        sql_preview = report[0].get("inputs", {}).get("sql", "N/A")
        print(f"SQL 语句: {sql_preview}")
        print(f"风险级别: {risk_info.get('risk_level', 'UNKNOWN')}")
        print(f"原因: {risk_info.get('reason', 'N/A')}")
        print(f"操作类型: {risk_info.get('sql_type', 'UNKNOWN')}")
    
    # 安全地获取预估行数
    estimated_rows = -1
    if len(report) > 1 and "outputs" in report[1]:
        estimated_rows = report[1].get("outputs", {}).get("estimated_rows", -1)
    
    if estimated_rows >= 0:
        print(f"预估受影响行数: {estimated_rows}")
    else:
        print("预估受影响行数: 无法估算")
    
    print("="*60)
    
    while True:
        choice = input("\n是否继续执行？(yes/no): ").strip().lower()
        if choice in ("yes", "y"):
            return True
        elif choice in ("no", "n"):
            return False
        else:
            print("请输入 yes 或 no")


def execute_sql_with_safety(raw_sql: str) -> Dict[str, Any]:
    """
    新的安全执行流程：
    ① risk_level = analyze_risk(sql, estimated_rows)
    ② dry_run()：只 estimate affected rows，不执行
    ③ 如果 risk = LOW → 直接执行 SQL
    ④ 如果 risk = MEDIUM / HIGH → 打印提示 → 等待用户 yes/no
    ⑤ 用户 yes → 创建 snapshot（自动事务或临时备份）
    ⑥ 执行 SQL
    ⑦ 写入 audit.json（由调用者处理）
    ⑧ 提供 replay 功能（回滚或重放）
    
    Args:
        sql: 要执行的 SQL 语句
        auto_confirm: 是否自动确认（用于测试或脚本）
    
    Returns:
        包含执行结果、风险信息、快照ID等的字典
    """
    audit_steps = []
    snapshot_id = None
    result = None
    risk_level = "UNKNOWN"
    risk_info = {}
    
    try:
        expression = sqlglot.parse_one(raw_sql)
    except Exception as e:
        # 如果 SQL 解析失败，使用默认值
        expression = None
        risk_info = {
            "risk_level": "UNKNOWN",
            "sql_type": "UNKNOWN",
            "reason": f"SQL 解析失败: {str(e)}"
        }
        risk_level = "UNKNOWN"

    # ① analyze_risk：分析风险等级
    analyze_risk_record = {
        "step_id": "step 1",
        "action": "analyze_risk",
        "start_at": datetime.datetime.utcnow().isoformat(),
        "inputs": {"sql": pretty(raw_sql)},
        "outputs": {},
        "status": "pending"
    }

    try:
        if expression:
            risk_info = analyze_risk(expression)
            risk_level = risk_info.get("risk_level", "UNKNOWN")
        analyze_risk_record["outputs"] = risk_info
        analyze_risk_record["status"] = "success"
    except Exception as e:
        analyze_risk_record["status"] = "error"
        analyze_risk_record["error"] = str(e)
        # 设置默认值
        if not risk_info:
            risk_info = {
                "risk_level": "UNKNOWN",
                "sql_type": "UNKNOWN",
                "reason": f"风险分析失败: {str(e)}"
            }
            risk_level = "UNKNOWN"
    finally:
        analyze_risk_record["end_at"] = datetime.datetime.utcnow().isoformat()
        audit_steps.append(analyze_risk_record)
    
    # ② dry_run：估计受影响行数
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
        dry_run_record["outputs"]["tables"] = get_tables(raw_sql)
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
    
    
    # ③ 根据风险级别决定是否执行
    if risk_level in ("LOW", "INFO"):
        # LOW 和 INFO 风险直接执行
        execute_record = {
            "step_id": "step 3",
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
            print(f"✅ SQL 执行成功")
        except Exception as e:
            execute_record["status"] = "error"
            execute_record["error"] = str(e)
            print(f"❌ SQL 执行失败: {str(e)}")
        finally:
            execute_record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(execute_record)
    elif risk_level == "UNKNOWN" or risk_level == "unknown":
        # 未知类型，拒绝执行
        execute_record = {
            "step_id": "step 3",
            "action": "execute_sql",
            "start_at": datetime.datetime.utcnow().isoformat(),
            "inputs": {"sql": pretty(raw_sql)},
            "outputs": {},
            "status": "error"
        }
        execute_record["error"] = "无法识别的 SQL 类型，拒绝执行"
        execute_record["end_at"] = datetime.datetime.utcnow().isoformat()
        audit_steps.append(execute_record)
        print(f"❌ 无法识别的 SQL 类型，拒绝执行")
    else:
        # MEDIUM, HIGH, CRITICAL 需要用户确认
        user_confirmed = cli_user_confirmation(audit_steps)

        if not user_confirmed:
            confirmation_record = {
                "step_id": "step 3",
                "action": "User_confirmation",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "user_choice": "No",
                "status": "cancelled"
            }
            confirmation_record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(confirmation_record)
            print("❌ 用户取消执行")
        else:
            confirmation_record = {
                "step_id": "step 3",
                "action": "User_confirmation",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "user_choice": "Yes",
                "status": "success"
            }
            confirmation_record["end_at"] = datetime.datetime.utcnow().isoformat()
            audit_steps.append(confirmation_record)

            snapshot_record = {
                "step_id": "step 4",
                "action": "create_snapshot",
                "start_at": datetime.datetime.utcnow().isoformat(),
                "inputs": {},
                "outputs": {},
                "status": "pending"
            }
            try:
                from poc.utils.snapshot_manager import create_snapshot_for_operation
                sql_type = risk_info.get("sql_type", "UNKNOWN")
                snapshot_meta = create_snapshot_for_operation(
                    operation_type=sql_type,
                    sql=raw_sql
                )
                # create_snapshot_for_operation 返回 snapshot_meta 字典
                if isinstance(snapshot_meta, dict):
                    snapshot_id = snapshot_meta.get("snapshot_id")
                else:
                    # 如果返回的是字符串（旧版本兼容）
                    snapshot_id = snapshot_meta
                snapshot_record["inputs"] = {"sql": raw_sql}
                snapshot_record["outputs"] = {"snapshot_id": snapshot_id} if snapshot_id else {}
                snapshot_record["status"] = "success"
                if snapshot_id:
                    print(f"✅ 已创建快照: {snapshot_id}")
            except Exception as e:
                snapshot_record["status"] = "error"
                snapshot_record["error"] = str(e)
                print(f"⚠️ 警告: 创建快照失败: {str(e)}")
            finally:
                snapshot_record["end_at"] = datetime.datetime.utcnow().isoformat()
                audit_steps.append(snapshot_record)
        

            execute_record = {
                "step_id": "step 5",
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
                print(f"✅ SQL 执行成功")
            except Exception as e:
                execute_record["status"] = "error"
                execute_record["error"] = str(e)
                print(f"❌ SQL 执行失败: {str(e)}")
            finally:
                execute_record["end_at"] = datetime.datetime.utcnow().isoformat()
                audit_steps.append(execute_record)
    
    # 生成总结
    timestamp = datetime.datetime.utcnow().strftime("%Y年%m月%d日 %H:%M:%S")
    operation_type = risk_info.get("sql_type", "UNKNOWN")
    
    if result:
        if len(result) > 0:
            first = result[0]
            if first:
                n = list(first.values())[0] if first else len(result)
                summary = f"{timestamp}，用户执行了{operation_type}操作，返回结果：{n}"
            else:
                summary = f"{timestamp}，用户执行了{operation_type}操作，返回 {len(result)} 行"
        else:
            summary = f"{timestamp}，用户执行了{operation_type}操作，无结果返回"
    else:
        summary = f"{timestamp}，用户执行了{operation_type}操作"
    
    if snapshot_id:
        summary += f"（快照ID: {snapshot_id}）"
    
    
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
    # 示例 SQL
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
    # 使用 LangGraph 框架（方案二）
    result = execute_sql_with_safety(sql)