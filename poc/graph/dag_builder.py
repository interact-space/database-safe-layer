from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Dict, Any, Literal
from poc.execution.executor import execute_sql_with_safety
import os

class PipelineState(TypedDict, total=False):
    """新的 PipelineState：直接从 SQL 开始"""
    sql: str  # 用户输入的 SQL
    estimated_rows: int
    risk: Dict[str, Any]
    snapshot_id: str
    result: List[Dict[str, Any]]
    execution_dag: List[Dict[str, Any]]
    summary: str
    auto_confirm: bool  # 是否自动确认（用于测试）

def node_execute_sql(state: PipelineState) -> PipelineState:
    """
    执行 SQL 的安全流程节点
    包含：dry_run → risk_analysis → 执行/确认 → snapshot → 执行
    """
    sql = state.get("sql", "")
    auto_confirm = state.get("auto_confirm", False)
    
    # 调用新的安全执行函数
    result = execute_sql_with_safety(sql, auto_confirm=auto_confirm)
    
    # 更新状态
    state["estimated_rows"] = result.get("estimated_rows", -1)
    state["risk"] = result.get("risk", {})
    state["snapshot_id"] = result.get("snapshot_id")
    state["result"] = result.get("result")
    state["execution_dag"] = result.get("audit_steps", [])
    state["summary"] = result.get("summary", "")
    
    return state

def build_graph():
    """
    构建新的 LangGraph 工作流
    流程：SQL 输入 → 安全执行（包含所有步骤）
    """
    graph = StateGraph(PipelineState)
    graph.add_node("execute", node_execute_sql)
    
    graph.set_entry_point("execute")
    graph.add_edge("execute", END)
    
    return graph.compile()