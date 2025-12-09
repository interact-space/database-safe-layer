import os, json, datetime
from dotenv import load_dotenv
from db_safe_layer.execution.executor import execute_sql_with_safety
from db_safe_layer.audit.log_manager import save_run
from db_safe_layer.audit.replay import replay
import argparse


load_dotenv()

def safe_exec(sql: str):
    """
        New process: accept SQL input directly
        ① dry_run(): only estimate affected rows, not executed
        ② risk_level = analyze_risk(sql, estimated_rows)
        ③ If risk = LOW → execute SQL directly
        ④ If risk = MEDIUM /HIGH → Print prompt → Wait for user yes/no
        ⑤ User yes → Create snapshot (automatic transaction or temporary backup)
        ⑥Execute SQL
        ⑦ Write audit.json
        ⑧ Provide replay function (rollback or replay)
        
        Args:
            sql: SQL statement to be executed
            use_graph: whether to use LangGraph framework (default True, use LangGraph)
        """
    result = execute_sql_with_safety(sql)
    
   #Organization audit JSON
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
    print("🚀 Starting SQL Safety Pipeline ...")
    # SQL example
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
    CREATE TABLE person_1 (
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

    parser = argparse.ArgumentParser(description="Safe DB Layer – A secure SQL execution layer")

    parser.add_argument("input_sql",type=str ,help="SQL input")


    args = parser.parse_args()

    run_id, run_obj = safe_exec(args.input_sql)
    # Replay
    # print("🔁 Replay now...")
    # re = replay(run_id)
    # print(json.dumps(re, ensure_ascii=False, indent=2))
