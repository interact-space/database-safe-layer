"""
简化的快照管理器 - 主要用于跟踪高风险操作
"""
import os
import json
import datetime
from typing import Optional, Dict, Any
from poc.db.snapshot import create_snapshot

SNAPSHOTS_LOG = os.path.join(os.getcwd(), "poc", "snapshots", "snapshots_log.json")
os.makedirs(os.path.dirname(SNAPSHOTS_LOG), exist_ok=True)


def create_snapshot_for_operation(operation_type: str, sql: str) -> str:
    """
    为高风险操作创建快照
    
    Returns:
        snapshot_id
    """
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snapshot_id = f"SNAPSHOT_{ts}"
    
    try:
        snapshot_meta = create_snapshot(snapshot_id)
        
        # 记录快照日志
        log_entry = {
            "snapshot_id": snapshot_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "operation_type": operation_type,
            "sql_preview": sql[:200] if sql else None
        }
        
        # 读取现有日志
        if os.path.exists(SNAPSHOTS_LOG):
            with open(SNAPSHOTS_LOG, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []
        
        logs.append(log_entry)
        
        # 保存日志
        with open(SNAPSHOTS_LOG, "w", encoding="utf-8") as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
        
        return snapshot_meta
        
    except Exception as e:
        print(f"⚠️ Warning: Failed to create snapshot: {str(e)}")
        return snapshot_id  # 仍然返回ID，即使快照创建失败


def get_snapshot_info(snapshot_id: str) -> Optional[Dict[str, Any]]:
    """获取快照信息"""
    if os.path.exists(SNAPSHOTS_LOG):
        with open(SNAPSHOTS_LOG, "r", encoding="utf-8") as f:
            logs = json.load(f)
            for log in logs:
                if log.get("snapshot_id") == snapshot_id:
                    return log
    return None

