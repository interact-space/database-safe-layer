from sqlglot import parse_one
from datetime import date
from typing import List, Dict, Any

from poc.intent.schema import FeasibilityIntent

# OMOP 概念映射（与 executor.py 中的映射保持一致）
CONDITION_CONCEPT_MAP = {
    "type 2 diabetes": 319835,
    "t2dm": 319835,
    "diabetes": 319835,
    "hypertension": 201826,
}

GENDER_CONCEPT_MAP = {
    "M": 8507,  # Male
    "F": 8532,  # Female
    "male": 8507,
    "female": 8532,
}


# =====================================================
# Helper: 拼接 WHERE 条件
# =====================================================

def build_where_clauses(intent: FeasibilityIntent, extra_fields: Dict[str, Any] = None) -> List[str]:
    """
    根据 intent 构造 WHERE + JOIN 的过滤条件
    使用 OMOP CDM 标准列名
    
    Args:
        intent: FeasibilityIntent 对象
        extra_fields: 包含额外字段的字典（如 condition_concept_id）
    """

    conditions = []
    extra = extra_fields or {}

    # -----------------------
    # 1）Condition 诊断 - 使用 condition_concept_id
    # -----------------------
    # 注意：condition_concept_id 在 extra_fields 中（从 resolve_concepts 设置）
    if extra.get("condition_concept_id"):
        conditions.append(f"c.condition_concept_id = {extra['condition_concept_id']}")
    elif intent.condition:
        # 如果没有 concept_id，尝试使用条件名称映射
        # 这里简化处理，实际应该查询 concept 表
        cond_lower = intent.condition.lower()
        concept_id = CONDITION_CONCEPT_MAP.get(cond_lower)
        if concept_id:
            conditions.append(f"c.condition_concept_id = {concept_id}")
        else:
            # 如果找不到映射，跳过条件过滤（或抛出错误）
            pass

    # -----------------------
    # 2）时间窗口 - 使用 condition_start_date
    # -----------------------
    if intent.time_window_start and intent.time_window_end:
        conditions.append(
            f"c.condition_start_date BETWEEN '{intent.time_window_start}' AND '{intent.time_window_end}'"
        )

    # -----------------------
    # 3）性别 - 使用 gender_concept_id
    # -----------------------
    # 优先使用 extra_fields 中的 demographic_filters（可能包含 gender_concept_id）
    demo_filters = extra.get("demographic_filters") if extra and extra.get("demographic_filters") else (intent.demographic_filters or {})
    
    if demo_filters:
        gender_concept_id = demo_filters.get("gender_concept_id")
        if gender_concept_id:
            conditions.append(f"p.gender_concept_id = {gender_concept_id}")
        elif demo_filters.get("gender"):
            # 如果没有 concept_id，使用默认映射
            gender = demo_filters["gender"]
            gender_id = GENDER_CONCEPT_MAP.get(gender.upper() if isinstance(gender, str) else str(gender).upper())
            if gender_id:
                conditions.append(f"p.gender_concept_id = {gender_id}")

    # -----------------------
    # 4）年龄段
    # -----------------------
    if demo_filters and demo_filters.get("age_range"):
        low, high = demo_filters["age_range"]
        current_year = date.today().year

        if low is not None:
            birth_year_high = current_year - low
            conditions.append(f"p.year_of_birth <= {birth_year_high}")

        if high is not None:
            birth_year_low = current_year - high
            conditions.append(f"p.year_of_birth >= {birth_year_low}")

    # -----------------------
    # 5）住院/门诊 - 需要 JOIN visit_occurrence 表
    # -----------------------
    if intent.visit_type:
        # 这里简化处理，实际需要 JOIN visit_occurrence
        # conditions.append(f"v.visit_type_concept_id = ...")
        pass

    return conditions


# =====================================================
# SQL 模板：Count
# =====================================================

def generate_count_sql(intent: FeasibilityIntent, extra_fields: Dict[str, Any] = None) -> str:
    where_clauses = build_where_clauses(intent, extra_fields)
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
    SELECT COUNT(*) AS count
    FROM condition_occurrence c
    JOIN person p ON p.person_id = c.person_id
    WHERE {where_sql}
    """
    return sql.strip()


# =====================================================
# SQL 模板：Trend（按年）
# =====================================================

def generate_trend_sql(intent: FeasibilityIntent, extra_fields: Dict[str, Any] = None) -> str:
    where_clauses = build_where_clauses(intent, extra_fields)
    where_sql = " AND ".join(where_clauses)

    group_unit = intent.group_by[0] if intent.group_by else "year"

    # PostgreSQL 使用 EXTRACT 或 TO_CHAR，SQLite 使用 strftime
    # 这里使用 PostgreSQL 兼容的语法
    if group_unit == "year":
        group_expr = "EXTRACT(YEAR FROM c.condition_start_date)"
    elif group_unit == "month":
        group_expr = "TO_CHAR(c.condition_start_date, 'YYYY-MM')"
    else:
        group_expr = "EXTRACT(YEAR FROM c.condition_start_date)"

    sql = f"""
    SELECT {group_expr} AS period,
           COUNT(*) AS count
    FROM condition_occurrence c
    JOIN person p ON p.person_id = c.person_id
    WHERE {where_sql}
    GROUP BY period
    ORDER BY period
    """
    return sql.strip()


# =====================================================
# SQL 模板：Distribution (年龄/性别)
# =====================================================

def generate_distribution_sql(intent: FeasibilityIntent, extra_fields: Dict[str, Any] = None) -> str:
    where_clauses = build_where_clauses(intent, extra_fields)
    where_sql = " AND ".join(where_clauses)

    sql = f"""
    SELECT p.gender_concept_id,
           COUNT(*) AS count
    FROM condition_occurrence c
    JOIN person p ON p.person_id = c.person_id
    WHERE {where_sql}
    GROUP BY p.gender_concept_id
    """
    return sql.strip()


# =====================================================
# 主函数：Intent → SQL
# =====================================================

def intent_to_sql(intent: FeasibilityIntent, extra_fields: Dict[str, Any] = None) -> str:
    """
    根据 intent.task_type 调用对应 SQL 模板
    
    Args:
        intent: FeasibilityIntent 对象
        extra_fields: 包含额外字段的字典（如 condition_concept_id）
    """

    if intent.task_type == "count":
        return generate_count_sql(intent, extra_fields)

    if intent.task_type == "trend":
        return generate_trend_sql(intent, extra_fields)

    if intent.task_type == "distribution":
        return generate_distribution_sql(intent, extra_fields)

    raise ValueError(f"Unsupported task_type: {intent.task_type}")


# =====================================================
# SQLGlot Risk Check & Dry-run
# =====================================================

def dry_run_sql(sql: str) -> str:
    """将 SQL 重写为: SELECT COUNT(*) FROM (原SQL)"""
    return f"SELECT COUNT(*) FROM ({sql}) AS t;"


def analyze_risk(sql: str) -> str:
    """基础风险检查：禁止 DELETE, UPDATE, DROP"""

    ast = parse_one(sql)

    if ast.find("DELETE") or ast.find("UPDATE") or ast.find("DROP"):
        return "high"

    return "low"


# =====================================================
# 测试
# =====================================================

if __name__ == "__main__":
    from poc.intent.schema import FeasibilityIntent

    intent = FeasibilityIntent(
        task_type="count",
        condition="diabetes",
        demographic_filters={"gender": "F"},
        time_window_start="2020-01-01",
        time_window_end="2023-01-01"
    )

    sql = intent_to_sql(intent)
    print("Generated SQL:")
    print(sql)

    print("\nDry-run:")
    print(dry_run_sql(sql))
