import os, json, datetime
from dotenv import load_dotenv
from poc.execution.executor import execute_sql_with_safety
from poc.audit.log_manager import save_run
from poc.audit.replay import replay


load_dotenv()

def run_pipeline(sql: str, use_graph: bool = True):
    """
    新的流程：直接接受 SQL 输入（使用 LangGraph 框架）
    ① dry_run()：只 estimate affected rows，不执行
    ② risk_level = analyze_risk(sql, estimated_rows)
    ③ 如果 risk = LOW → 直接执行 SQL
    ④ 如果 risk = MEDIUM / HIGH → 打印提示 → 等待用户 yes/no
    ⑤ 用户 yes → 创建 snapshot（自动事务或临时备份）
    ⑥ 执行 SQL
    ⑦ 写入 audit.json
    ⑧ 提供 replay 功能（回滚或重放）
    
    Args:
        sql: 要执行的 SQL 语句
        use_graph: 是否使用 LangGraph 框架（默认 True，使用 LangGraph）
    """
    if use_graph:
        # 使用 LangGraph 框架
        from poc.graph.dag_builder import build_graph
        graph = build_graph()
        result_state = graph.invoke({"sql": sql, "auto_confirm": False})
        
        # 从状态中提取结果
        result = {
            "sql": sql,
            "estimated_rows": result_state.get("estimated_rows", -1),
            "risk": result_state.get("risk", {}),
            "snapshot_id": result_state.get("snapshot_id"),
            "result": result_state.get("result"),
            "audit_steps": result_state.get("execution_dag", []),
            "summary": result_state.get("summary", "")
        }
    else:
        # 直接调用函数（默认方式，更简单）
        result = execute_sql_with_safety(sql)
    
    # 组织审计 JSON
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_id = f"RUN_{ts}"
    run_obj = {
        "run_id": run_id,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "sql": sql,
        "estimated_rows": result.get("estimated_rows"),
        "risk_level": result.get("risk"),
        "snapshot_id": result.get("snapshot_id"),
        "execution_result": result.get("result"),
        "execution_dag": result.get("audit_steps", []),
        "summary": result.get("summary", ""),
        "env": {
            "db_url": os.getenv("DATABASE_URL", "")
        }
    }
    run_id, path = save_run(run_obj)
    print(f"✅ Run saved: {path}")
    print(f"🧾 Summary: {run_obj['summary']}")
    return run_id, run_obj

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
    sql1 ="""
    CREATE TABLE person_copy (
    person_id INT PRIMARY KEY,
    gender_concept_id INT,
    year_of_birth INT,
    race_concept_id INT,
    ethnicity_concept_id INT,
    location_id INT,
    provider_id INT,
    care_site_id INT,
    person_source_value VARCHAR(50)
);
"""
    sql2 ="""
    INSERT INTO person (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id, person_source_value)
    VALUES 
    (7, 8532, 1990, 8527, 38003563, 'P007'),
    (6, 8507, 1975, 8516, 38003564, 'P006'),
    (4, 8532, 2000, 8515, 38003563, 'P004');
"""
    sql3 ="""
        SELECT * FROM person;
    """
    sql4 ="""
        SELECT person_id, year_of_birth FROM person WHERE year_of_birth > 1980;
    """
    sql5 ="""
        SELECT * FROM person WHERE gender_concept_id = 8507 ORDER BY year_of_birth DESC;
    """
    sql6 ="""
        UPDATE person SET year_of_birth = 1991 WHERE person_id = 2;
    """
    sql7 ="""
        UPDATE person SET location_id = 999;
    """
    sql8 ="""
        DELETE FROM person WHERE person_id = 4;
    """
    sql9 ="""
       DELETE FROM person WHERE year_of_birth < 1980;
    """
    sql10 ="""
       DROP TABLE person;
    """

    # 使用 LangGraph 框架（方案二）
    run_id, run_obj = run_pipeline(sql9, use_graph=False)
    # Replay
    # print("🔁 Replay now...")
    # re = replay(run_id)
    # print(json.dumps(re, ensure_ascii=False, indent=2))
