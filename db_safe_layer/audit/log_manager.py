import os, json, datetime
from dotenv import load_dotenv
load_dotenv()

RUNS_DIR = os.path.join(os.getcwd(), "db_safe_layer", "runs") if os.getcwd().endswith("db_safe_layer") else os.path.join(os.getcwd(), "runs")
os.makedirs(RUNS_DIR, exist_ok=True)

def save_run(run_obj: dict) -> str:
    filename = f"{run_obj['run_id']}.json"
    path = os.path.join(RUNS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(run_obj, f, ensure_ascii=False, indent=2)
    return run_obj["run_id"], path

def load_run(run_id: str) -> dict:
    path = os.path.join(RUNS_DIR, f"{run_id}.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
