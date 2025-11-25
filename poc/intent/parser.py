import os, json 
import re
from .schema import FeasibilityIntent, ParseContext
from poc.utils.llm_client import get_llm
from dotenv import load_dotenv
from datetime import date
today_str = date.today().strftime("%Y-%m-%d")

load_dotenv()

SYSTEM_TMPL = """
You are an expert in clinical research data modeling. 
Your job is to convert natural language research questions from hospital experts 
into a normalized **JSON intent object**.

Return **JSON only**. No explanation.

Your JSON must follow this schema (fields may be null if not mentioned):

{{
  "task_type": "count | distribution | trend | compare | cohort | stats",
  "condition": null,
  "drug": null,
  "procedure": null,
  "visit_type": null,
  "demographic_filters": null,
  "time_window_start": null,
  "time_window_end": null,
  "group_by": null,
  "metric": null,
  "research_question": null
}}

### Interpretation Rules:

1. Determine the main task:
   - "多少" → count
   - "趋势" → trend
   - "分布" → distribution
   - "比较" → compare
   - "队列" / “cohort” → cohort
   - "平均" → stats (avg)
   - “超过/大于/小于” → stats (filter)

2. Condition synonyms:
   - “糖尿病” → "type 2 diabetes"
   - “高血压” → hypertension
   - “心衰” → heart failure

3. Time ranges:
   - Identify year mentions (“2019–2024” → "2019-01-01" to "2024-12-31")
   - Identify expressions (“过去半年”, “去年”) and convert to dates.

4. Drugs:
   - Identify drug names (metformin, insulin, etc.)

5. Demographics:
   - If the question mentions gender/age, populate demographic_filters:
     Example:
     {{
       "gender": "M",
       "age_range": [40,60]
     }}

6. Grouping:
   - Identify “按…分组 / 分性别 / 按年份统计” → group_by list

7. Always echo the user’s question in research_question.

OMOP_VERSION: {omop_version}

Return only valid JSON that can be parsed by Python json.loads().
TODAY = {today_str}
"""


USER_TMPL = """
Convert the following user research question into the JSON intent object:

User request: "{user_query}"
Return JSON only.
"""


def parse_intent(user_query: str, ctx: ParseContext) -> FeasibilityIntent:
    client, model = get_llm()
    sys = SYSTEM_TMPL.format(omop_version=ctx.omop_version,today_str=today_str)
    usr = USER_TMPL.format(user_query=user_query)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": sys},
            {"role": "user", "content": usr}
        ],
        temperature=0.9,
    )
    raw = resp.choices[0].message.content.strip()
    try:
        data = json.loads(raw)
    except Exception:
        # fallback: try to find the first JSON block

        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            raise ValueError(f"LLM did not return JSON: {raw}")
        data = json.loads(m.group(0))

    intent = FeasibilityIntent(**data)

    # ===========================
    #  Post-processing Enhancements
    # ===========================

    # -------------------------
    # 1. trend → 自动 group_by
    # -------------------------
    if intent.task_type == "trend" and not intent.group_by:
        intent.group_by = ["year"]

    # -------------------------
    # 2. 年龄识别
    # -------------------------
    age_patterns = [
        (r'(\d+)\s*[-~到至]\s*(\d+)\s*岁', lambda a, b: [int(a), int(b)]),
        (r'大于\s*(\d+)\s*岁', lambda a: [int(a), None]),
        (r'超过\s*(\d+)\s*岁', lambda a: [int(a), None]),
        (r'(\d+)\s*岁以上', lambda a: [int(a), None]),
        (r'小于\s*(\d+)\s*岁', lambda a: [None, int(a)]),
        (r'低于\s*(\d+)\s*岁', lambda a: [None, int(a)]),
    ]

    for pattern, handler in age_patterns:
        m = re.search(pattern, user_query)
        if m:
            intent.demographic_filters = intent.demographic_filters or {}
            if len(m.groups()) == 2:
                intent.demographic_filters["age_range"] = handler(m.group(1), m.group(2))
            else:
                intent.demographic_filters["age_range"] = handler(m.group(1))
            break

    # 常见年龄关键词
    if "儿童" in user_query or "小孩" in user_query:
        intent.demographic_filters = intent.demographic_filters or {}
        intent.demographic_filters["age_range"] = [0, 14]

    if "成年人" in user_query:
        intent.demographic_filters = intent.demographic_filters or {}
        intent.demographic_filters["age_range"] = [18, 65]

    if "老年" in user_query or "高龄" in user_query:
        intent.demographic_filters = intent.demographic_filters or {}
        intent.demographic_filters["age_range"] = [65, None]

    # -------------------------
    # 3. 性别识别增强
    # -------------------------
    gender_map = {
        "男": "M", "男性": "M", "男人": "M", "male": "M", "m": "M",
        "女": "F", "女性": "F", "女人": "F", "female": "F", "f": "F",
    }

    for k, v in gender_map.items():
        if k in user_query:
            intent.demographic_filters = intent.demographic_filters or {}
            intent.demographic_filters["gender"] = v
            break

    return intent

if __name__ == "__main__":
    print(" ---test llm intent parse---")
    q = '找出过去半年住院超过 2 次的 COPD 患者'
    ctx = ParseContext(omop_version='omop5.4')

    result = parse_intent (q,ctx)
    print(f"result is: {result}")
