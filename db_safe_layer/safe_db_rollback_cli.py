# safe_db_rollback_cli.py

import click
from .rollback import rollback_to

@click.command("safe-db-rollback")
def main():
    """Rollback database to a snapshot."""
    rollback_to()

if __name__ == "__main__":
    main()
