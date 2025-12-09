# safe_db_cli.py
import click
from .app import safe_exec
import argparse

@click.command("safe-layer")
@click.argument("sql")
def main(sql):
    run_id, run_obj = safe_exec(sql)
    print(run_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Safe DB Layer – A secure SQL execution layer")

    parser.add_argument("input_sql",type=str ,help="SQL input")


    args = parser.parse_args()
    """Execute SQL with safety checks."""
    main(args.input_sql)
