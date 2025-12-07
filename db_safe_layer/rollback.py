# safe_db/rollback.py

from .db.snapshot import list_snapshots,rollback_to_snapshot

def rollback_to():
    """
    Rollback database to a previously created snapshot.

    Returns:
        dict: Result object.
    """
    print("1 List all available snapshots ...")
    snapshots = list_snapshots()
    for snapshot in snapshots:
        if snapshot['snapshot_id'] is not None:
            print(snapshot['snapshot_id'])
    print("""2 Roll back to the specified snapshot""")
    ans = input("\nSpecified snapshot id: ")
    if ans:
        result= rollback_to_snapshot(ans,True)
        print(result) 
 
    