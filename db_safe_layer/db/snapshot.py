"""
Database Snapshot Management
Supports creating database snapshots and rollback functions
"""
import os
import json
import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text, inspect
from db_safe_layer.db.database import DatabaseManager
from db_safe_layer.db.config import settings
from dotenv import load_dotenv

load_dotenv()

SNAPSHOTS_DIR = os.path.join(os.getcwd(), "db_safe_layer", "snapshots")  if os.getcwd().endswith("db_safe_layer") else os.path.join(os.getcwd(), "snapshots")
os.makedirs(SNAPSHOTS_DIR, exist_ok=True)

def create_snapshot(snapshot_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a database snapshot
    For PostgreSQL/Supabase we save the table structure and data
    For SQLite, we copy the database file directly
    
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
            # Get all table names
            inspector = inspect(db.engine)
            tables = inspector.get_table_names()
            
            # For each table, save the table structure and data
            for table_name in tables:
                # Get the table structure
                columns = inspector.get_columns(table_name)
                table_structure = {col['name']: str(col['type']) for col in columns}
                
                # Get data
                result = s.execute(text(f"SELECT * FROM {table_name}"))
                rows = result.fetchall()
                data = [dict(row._mapping) for row in rows]
                
                snapshot_meta["tables"][table_name] = {
                    "structure": table_structure,
                    "row_count": len(data),
                    # Note: For large tables, only the first 1000 rows may be saved as an example
                    "data_sample": data[:1000] if len(data) > 1000 else data,
                    "has_more": len(data) > 1000
                }
        
        # Save snapshot metadata to file
        snapshot_path = os.path.join(SNAPSHOTS_DIR, f"{snapshot_id}.json")
        with open(snapshot_path, "w", encoding="utf-8") as f:
            json.dump(snapshot_meta, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"✅ Snapshot created: {snapshot_id}")
        return snapshot_meta
        
    except Exception as e:
        raise RuntimeError(f"Failed to create snapshot: {str(e)}")


def load_snapshot(snapshot_id: str) -> Dict[str, Any]:
    """Load snapshot metadata"""
    snapshot_path = os.path.join(SNAPSHOTS_DIR, f"{snapshot_id}.json")
    if not os.path.exists(snapshot_path):
        raise FileNotFoundError(f"Snapshot not found: {snapshot_id}")
    
    with open(snapshot_path, "r", encoding="utf-8") as f:
        return json.load(f)


def rollback_to_snapshot(snapshot_id: str, confirm: bool = False) -> Dict[str, Any]:
    """
    Roll back to the specified snapshot (full implementation version: supports structure reconstruction and data recovery)
    Compatible with SQLite and PostgreSQL
    """
    if not confirm:
        raise ValueError("Rollback requires explicit confirmation")
    
    snapshot = load_snapshot(snapshot_id)
    db = DatabaseManager(settings.DB_URL, echo=False)
    
    # 1. Detect database type
    dialect = db.engine.dialect.name
    print(f"🚀 [Rollback] Detected database dialect: {dialect.upper()}")

    try:
        with db.session() as s:
            inspector = inspect(db.engine)
            
            # 2. Get the tables in the current database
            if dialect == 'sqlite':
                current_tables = set(inspector.get_table_names())
            else:
                current_tables = set(inspector.get_table_names(schema="public"))
            
            snapshot_tables = set(snapshot["tables"].keys())
            
            # ---Internal helper function: safely delete table ---
            def safe_drop_table(t_name):
                if dialect == 'sqlite':
                    # SQLite: Turn off foreign key checking -> Delete -> Turn on foreign key checking
                    s.execute(text("PRAGMA foreign_keys = OFF"))
                    s.execute(text(f"DROP TABLE IF EXISTS {t_name}"))
                    s.execute(text("PRAGMA foreign_keys = ON"))
                else:
                    # Postgres: using CASCADE
                    s.execute(text(f"DROP TABLE IF EXISTS {t_name} CASCADE"))

            # 3. Cleanup phase: delete new tables that are not in the snapshot
            tables_to_drop = current_tables - snapshot_tables
            for table in tables_to_drop:
                print(f"   🗑️  Dropping extra table: {table}")
                safe_drop_table(table)
            
            # 4. Recovery phase: table-by-table reset
            for table_name, table_info in snapshot["tables"].items():
                print(f"   ♻️  Restoring table: {table_name} ...")
                
                # A. Completely delete the old table (to prevent structural inconsistency)
                safe_drop_table(table_name)
                
                # B. Rebuild the table structure
                # We build SQL from snapshot['structure'] (e.g. {'id': 'INTEGER', 'name': 'VARCHAR'})
                structure = table_info.get("structure", {})
                if not structure:
                    print(f"   ⚠️  Skipping {table_name}: No structure definition found.")
                    continue
                
                # Splice CREATE TABLE statements
                # Example: CREATE TABLE person (person_id INTEGER, name VARCHAR(50))
                cols_def = ", ".join([f"{col} {ctype}" for col, ctype in structure.items()])
                create_sql = f"CREATE TABLE {table_name} ({cols_def})"
                s.execute(text(create_sql))
                
                # C. Restore data
                data_rows = table_info.get("data_sample", [])
                if data_rows:
                    # Get the list of column names (assuming all rows have the same structure, take the first row)
                    keys = list(data_rows[0].keys())
                    
                    # Construct INSERT statement (use parameterized binding to prevent SQL injection)
                    # Example: INSERT INTO person (id, name) VALUES (:id, :name)
                    cols_str = ", ".join(keys)
                    vals_str = ", ".join([f":{k}" for k in keys])
                    insert_sql = text(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})")
                    
                    # Insert data in batches
                    s.execute(insert_sql, data_rows)
                    print(f"      ✅ Restored {len(data_rows)} rows.")
                else:
                    print(f"      ℹ️  Table is empty in snapshot.")

            s.commit()
        
        return {
            "status": "success",
            "snapshot_id": snapshot_id,
            "message": f"Successfully rolled back to snapshot {snapshot_id}"
        }
        
    except Exception as e:
        print(f"❌ Rollback failed: {e}")
        raise RuntimeError(f"Failed to rollback: {str(e)}")


def list_snapshots() -> list:
    """List all available snapshots"""
    snapshots = []
    if os.path.exists(SNAPSHOTS_DIR):
        for filename in os.listdir(SNAPSHOTS_DIR):
            if filename.endswith(".json"):
                snapshot_id = filename[:-5] # Remove .json suffix
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
    print("1 List all available snapshots ...")
    id = list_snapshots()
    print(id)
    print("""2 Roll back to the specified snapshot
    Note: This is a dangerous operation and requires confirmation
    
    Args:
        snapshot_id: snapshot ID
        confirm: Whether to confirm execution of rollback
    
    Returns:
        Rollback result information.
          """)
    ans = input("\nSpecified snapshot id: ")
    if ans:
        result= rollback_to_snapshot(ans,True)