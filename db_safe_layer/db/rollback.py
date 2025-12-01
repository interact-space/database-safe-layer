# safe_db/rollback.py

from .snapshot import list_snapshots,rollback_to_snapshot

def rollback_to():
    """
    Rollback database to a previously created snapshot.

    Returns:
        dict: Result object.
    """
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
        return result
    else:
        return None
    