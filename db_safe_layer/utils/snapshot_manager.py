"""
Simplified Snapshot Manager -primarily for tracking high-risk operations
"""
import os
import json
import datetime
from typing import Optional, Dict, Any
from db_safe_layer.db.snapshot import create_snapshot

SNAPSHOTS_LOG = os.path.join(os.getcwd(), "db_safe_layer", "snapshots", "snapshots_log.json")
os.makedirs(os.path.dirname(SNAPSHOTS_LOG), exist_ok=True)


def create_snapshot_for_operation(operation_type: str, sql: str) -> str:
    """
   Create snapshots of high-risk operations
    
    Returns:
        snapshot_id
    """
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snapshot_id = f"SNAPSHOT_{ts}"
    
    try:
        snapshot_meta = create_snapshot(snapshot_id)
        
        # Record snapshot log
        log_entry = {
            "snapshot_id": snapshot_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "operation_type": operation_type,
            "sql_preview": sql[:200] if sql else None
        }
        
        # Read existing logs
        if os.path.exists(SNAPSHOTS_LOG):
            with open(SNAPSHOTS_LOG, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []
        
        logs.append(log_entry)
        
        # Save log
        with open(SNAPSHOTS_LOG, "w", encoding="utf-8") as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
        
        return snapshot_meta
        
    except Exception as e:
        print(f"⚠️ Warning: Failed to create snapshot: {str(e)}")
        return snapshot_id  # Still return ID even if snapshot creation fails


def get_snapshot_info(snapshot_id: str) -> Optional[Dict[str, Any]]:
    """Get snapshot information"""
    if os.path.exists(SNAPSHOTS_LOG):
        with open(SNAPSHOTS_LOG, "r", encoding="utf-8") as f:
            logs = json.load(f)
            for log in logs:
                if log.get("snapshot_id") == snapshot_id:
                    return log
    return None

