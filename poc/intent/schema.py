from pydantic import BaseModel, Field
from typing import Optional, List, Dict

class FeasibilityIntent(BaseModel):
    task_type: Optional[str] = Field(description="count / distribution / trend / compare / cohort / stats")
    
    # 研究对象相关（人群定义）
    condition: Optional[str] = None
    drug: Optional[str] = None
    procedure: Optional[str] = None
    visit_type: Optional[str] = None  # inpatient/outpatient/emergency
    demographic_filters: Optional[dict] = Field(
        default=None,
        description="e.g. {'gender': 'M', 'age_range':[40,60]}"
    )  

    # 时间范围
    time_window_start: Optional[str] = None
    time_window_end: Optional[str] = None

    # 分组/分析维度
    group_by: Optional[list] = None   # ['gender', 'year', 'age_group']

    # 指标
    metric: Optional[str] = None  # "count" / "avg" / "sum" / "rate"

    # 研究目的（可用于解释）
    research_question: Optional[str] = None


class ParseContext(BaseModel):
    omop_version: str = "OMOP1"
