"""
Database Snapshot Management
支持创建数据库快照和回滚功能
"""
import os
import json
import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text, inspect
from poc.db.database import DatabaseManager
from poc.db.config import settings
from dotenv import load_dotenv

load_dotenv()

SNAPSHOTS_DIR = os.path.join(os.getcwd(), "poc", "snapshots")  if os.getcwd().endswith("poc") else os.path.join(os.getcwd(), "snapshots")
os.makedirs(SNAPSHOTS_DIR, exist_ok=True)

def create_snapshot(snapshot_id: Optional[str] = None) -> Dict[str, Any]:
    """
    创建数据库快照
    对于 PostgreSQL/Supabase，我们保存表结构和数据
    对于 SQLite，我们直接复制数据库文件
    
    Returns:
        Dict with snapshot_id and metadata
    """
    if snapshot_id is None:
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        snapshot_id = f"SNAPSHOT_{ts}"
    
    db = DatabaseManager(settings.DB_URL, echo=False)
    snapshot_meta = {
        "snapshot_id": snapshot_id,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "db_url": settings.DB_URL,
        "tables": {}
    }
    
    try:
        with db.session() as s:
            # 获取所有表名
            inspector = inspect(db.engine)
            tables = inspector.get_table_names()
            
            # 对于每个表，保存表结构和数据
            for table_name in tables:
                # 获取表结构
                columns = inspector.get_columns(table_name)
                table_structure = {col['name']: str(col['type']) for col in columns}
                
                # 获取数据
                result = s.execute(text(f"SELECT * FROM {table_name}"))
                rows = result.fetchall()
                data = [dict(row._mapping) for row in rows]
                
                snapshot_meta["tables"][table_name] = {
                    "structure": table_structure,
                    "row_count": len(data),
                    # 注意：对于大表，可能只保存前1000行作为示例
                    "data_sample": data[:1000] if len(data) > 1000 else data,
                    "has_more": len(data) > 1000
                }
        
        # 保存快照元数据到文件
        snapshot_path = os.path.join(SNAPSHOTS_DIR, f"{snapshot_id}.json")
        with open(snapshot_path, "w", encoding="utf-8") as f:
            json.dump(snapshot_meta, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"✅ Snapshot created: {snapshot_id}")
        return snapshot_meta
        
    except Exception as e:
        raise RuntimeError(f"Failed to create snapshot: {str(e)}")


def load_snapshot(snapshot_id: str) -> Dict[str, Any]:
    """加载快照元数据"""
    snapshot_path = os.path.join(SNAPSHOTS_DIR, f"{snapshot_id}.json")
    if not os.path.exists(snapshot_path):
        raise FileNotFoundError(f"Snapshot not found: {snapshot_id}")
    
    with open(snapshot_path, "r", encoding="utf-8") as f:
        return json.load(f)

def rollback_to_snapshot(snapshot_id: str, confirm: bool = False) -> Dict[str, Any]:
    """
    回滚到指定的快照 (PostgreSQL 专用版)
    策略：
    1. 清理：删除当前存在但快照中不存在的表。
    2. 恢复：对快照中的每张表，DROP 原表 (CASCADE)，然后从备份表重建。
    
    Args:
        snapshot_id: 快照ID
        confirm: 是否确认执行回滚
    
    Returns:
        回滚结果信息
    """
    if not confirm:
        raise ValueError("Rollback requires explicit confirmation")
    
    # 假设 load_snapshot 返回的结构如下：
    # {
    #   "tables": {
    #      "person": { 
    #          "backup_table_name": "backup_person_17012345", 
    #          "ddl": "CREATE TABLE person (...)" (可选)
    #      }
    #   }
    # }
    snapshot = load_snapshot(snapshot_id)
    url= settings.DB_URL
    db = DatabaseManager(settings.DB_URL, echo=False)
    current_dialect = db.engine.dialect.name
    print(f"\n[DEBUG] 当前数据库类型: {current_dialect}")
    print(f"[DEBUG] 连接字符串 (DB_URL): {settings.DB_URL}")
    
    try:
        with db.session() as s:
            # 1. 获取当前数据库状态
            inspector = inspect(db.engine)
            # 注意: 这里最好指定 schema，默认 public
            current_tables = set(inspector.get_table_names(schema="public"))
            snapshot_tables = set(snapshot["tables"].keys())
            
            # 2.【清理阶段】删除快照中不存在的"新增表"
            # 例如：快照后用户新建了一个 table_x，回滚时需要删掉它
            tables_to_drop = current_tables - snapshot_tables
            for table in tables_to_drop:
                print(f"Dropping new table: {table}")
                # PostgreSQL 关键：使用 CASCADE 级联删除依赖项（如外键、视图）
                s.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
            
            # 3.【恢复阶段】逐表恢复
            for original_table, table_info in snapshot["tables"].items():
                backup_table = table_info.get("backup_table_name")
                
                if not backup_table:
                    print(f"Warning: No backup table found for {original_table}, skipping.")
                    continue

                print(f"Restoring {original_table} from {backup_table}...")

                # A. 删除现有的脏表 (CASCADE 确保即使有外键也能删掉)
                s.execute(text(f"DROP TABLE IF EXISTS {original_table} CASCADE"))
                
                # B. 重建表并恢复数据
                # 方案 1 (推荐): 如果你保存了原始 DDL (CREATE TABLE 语句)
                # 优点：能完美恢复索引、默认值、约束、主键
                if "ddl" in table_info and table_info["ddl"]:
                    # 1. 执行原始建表语句
                    s.execute(text(table_info["ddl"]))
                    # 2. 插入数据
                    s.execute(text(f"INSERT INTO {original_table} SELECT * FROM {backup_table}"))
                
                # 方案 2 (兜底): 如果没有 DDL，直接用 CREATE AS SELECT
                # 缺点：会丢失主键(PK)、索引(Indexes)和默认值(Defaults)，只保留数据和列类型
                else:
                    s.execute(text(f"CREATE TABLE {original_table} AS SELECT * FROM {backup_table}"))
            
            s.commit()
        
        return {
            "status": "success",
            "snapshot_id": snapshot_id,
            "message": f"Successfully rolled back to snapshot {snapshot_id}"
        }
        
    except Exception as e:
        # 发生错误回滚事务
        # 注意：在某些数据库驱动中，DDL (DROP/CREATE) 可能会自动提交，但在 Postgres 事务块中通常是可以回滚的
        raise RuntimeError(f"Failed to rollback: {str(e)}")

# def rollback_to_snapshot(snapshot_id: str, confirm: bool = False) -> Dict[str, Any]:
#     """
#     回滚到指定的快照
#     注意：这是一个危险操作，需要确认
    
#     Args:
#         snapshot_id: 快照ID
#         confirm: 是否确认执行回滚
    
#     Returns:
#         回滚结果信息
#     """
#     if not confirm:
#         raise ValueError("Rollback requires explicit confirmation")
    
#     snapshot = load_snapshot(snapshot_id)
#     db = DatabaseManager(settings.DB_URL, echo=False)
    
#     try:
#         with db.session() as s:
#             # 获取当前所有表
#             inspector = inspect(db.engine)
#             current_tables = set(inspector.get_table_names())
#             snapshot_tables = set(snapshot["tables"].keys())
            
#             # 删除快照中不存在的表
#             tables_to_drop = current_tables - snapshot_tables
#             for table in tables_to_drop:
#                 s.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
            
#             # 恢复每个表
#             for table_name, table_info in snapshot["tables"].items():
#                 # 删除现有表
#                 s.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                
#                 # 重新创建表结构（简化版，实际应该使用DDL）
#                 # 注意：这里只是演示，实际应该保存完整的DDL语句
#                 structure = table_info["structure"]
                
#                 # 恢复数据
#                 if table_info.get("data_sample"):
#                     # 这里简化处理，实际应该根据表结构重建表
#                     # 由于我们保存的是数据样本，完整的回滚可能需要更复杂的逻辑
#                     pass
            
#             s.commit()
        
#         return {
#             "status": "success",
#             "snapshot_id": snapshot_id,
#             "message": f"Rolled back to snapshot {snapshot_id}"
#         }
        
#     except Exception as e:
#         raise RuntimeError(f"Failed to rollback: {str(e)}")


def list_snapshots() -> list:
    """列出所有可用的快照"""
    snapshots = []
    if os.path.exists(SNAPSHOTS_DIR):
        for filename in os.listdir(SNAPSHOTS_DIR):
            if filename.endswith(".json"):
                snapshot_id = filename[:-5]  # 移除 .json 后缀
                try:
                    snapshot = load_snapshot(snapshot_id)
                    snapshots.append({
                        "snapshot_id": snapshot_id,
                        "timestamp": snapshot.get("timestamp"),
                        "tables": list(snapshot.get("tables", {}).keys())
                    })
                except Exception:
                    continue
    
    return sorted(snapshots, key=lambda x: x.get("timestamp", ""), reverse=True)

if __name__ == "__main__":
    print("🚀 test snapshot ...")
    print("1 列出所有可用的快照 ...")
    id = list_snapshots()
    print(id)
    print("""2 回滚到指定的快照
    注意：这是一个危险操作，需要确认
    
    Args:
        snapshot_id: 快照ID
        confirm: 是否确认执行回滚
    
    Returns:
        回滚结果信息.
          """)
    ans = input("\n指定的快照ID: ")
    if ans:
        result= rollback_to_snapshot(ans,True)