from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from airflow.models import DagBag
from airflow.models.mappedoperator import MappedOperator


ROOT = Path(__file__).resolve().parents[1]
INPUT = Path(sys.argv[1]).resolve()
OUTPUT = Path(sys.argv[2]).resolve()
SOURCE_DAG = ROOT / "implementation/remote_tile_delivery.py"


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    required = {"README.md", "batch_registry.csv", "processing_contract.json", "release_request.json", "current_dag/remote_tile_delivery.py", "manifests/orbit-1842.csv", "manifests/orbit-1843.csv", "manifests/orbit-1843-patch.csv", "manifests/orbit-1844.csv"}
    actual = {path.relative_to(INPUT).as_posix() for path in INPUT.rglob("*") if path.is_file()}
    if actual != required:
        raise ValueError("输入成员与交付约定不一致")
    contract = json.loads((INPUT / "processing_contract.json").read_text(encoding="utf-8"))
    request = json.loads((INPUT / "release_request.json").read_text(encoding="utf-8"))
    with (INPUT / "batch_registry.csv").open(encoding="utf-8", newline="") as handle:
        batches = list(csv.DictReader(handle))
    if len({row["batch_id"] for row in batches}) != len(batches):
        raise ValueError("batch_id存在重复")
    tmp = OUTPUT.with_name(OUTPUT.name + ".building")
    if tmp.exists():
        shutil.rmtree(tmp)
    (tmp / "dags").mkdir(parents=True)
    shutil.copy2(SOURCE_DAG, tmp / "dags/remote_tile_delivery.py")
    os.environ["ALE_INPUT_ROOT"] = str(INPUT)
    bag = DagBag(dag_folder=str(tmp / "dags"), include_examples=False, safe_mode=False)
    if bag.import_errors:
        raise RuntimeError(json.dumps(bag.import_errors, ensure_ascii=False))
    dag = bag.dags[contract["dag_id"]]
    checkpoints = tmp / "checkpoints"
    airflow_home = tmp / "airflow-home"
    env = os.environ.copy()
    env.update({"AIRFLOW_HOME": str(airflow_home), "AIRFLOW__CORE__LOAD_EXAMPLES": "False", "PYTHONPATH": str(tmp / "dags"), "ALE_INPUT_ROOT": str(INPUT)})
    migrated = subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], env=env, text=True, capture_output=True, timeout=300)
    if migrated.returncode:
        raise RuntimeError(migrated.stdout + migrated.stderr)
    summaries = []
    for index, row in enumerate(batches, 1):
        conf = {"batch_id": row["batch_id"], "family": row["family"], "manifest_file": row["manifest_file"]}
        command = [sys.executable, "-c", "import json,os; from pendulum import datetime; from remote_tile_delivery import dag; dag.test(execution_date=datetime(2026,8,18,int(os.environ['ALE_HOUR']),tz='UTC'),run_conf=json.loads(os.environ['ALE_CONF']))"]
        run_env = {**env, "ALE_HOUR": str(index), "ALE_CONF": json.dumps(conf)}
        completed = subprocess.run(command, cwd=tmp / "dags", env=run_env, text=True, capture_output=True, timeout=300)
        if completed.returncode:
            raise RuntimeError(completed.stdout + completed.stderr)
        summaries.append(json.loads((tmp / "dags/summaries" / f'{row["batch_id"]}.json').read_text(encoding="utf-8")))
    ledger = []
    for path in sorted((tmp / "dags/events").glob("*/*.json")):
        ledger.append(json.loads(path.read_text(encoding="utf-8")))
    if (tmp / "dags/checkpoints").exists():
        shutil.move(str(tmp / "dags/checkpoints"), str(checkpoints))
    write_csv(tmp / "results/shard_ledger.csv", ledger, ["batch_id", "family", "map_index", "shard_id", "action", "outcome", "reason", "signature"])
    write_csv(tmp / "results/batch_summary.csv", summaries, ["batch_id", "family", "discovered_count", "processed_count", "reused_count", "rejected_count", "checkpoint_count", "release_status"])
    structure = {"dag_id": dag.dag_id, "task_ids": dag.task_ids, "mapped_task_ids": [task.task_id for task in dag.tasks if isinstance(task, MappedOperator)], "edges": sorted([{"upstream": task.task_id, "downstream": down.task_id} for task in dag.tasks for down in task.downstream_list], key=lambda row: (row["upstream"], row["downstream"]))}
    (tmp / "results/dag_structure.json").write_text(json.dumps(structure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ready = [row["batch_id"] for row in summaries if row["release_status"] == contract["ready_status"]]
    if contract["empty_batch_status"] != contract["ready_status"]:
        raise ValueError("空批次状态与下发状态不一致")
    (tmp / "release-summary.json").write_text(json.dumps({**request, "dag_id": contract["dag_id"], "ready_batches": ready, "candidate_dag_path": "output/dags/remote_tile_delivery.py"}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tool = """from __future__ import annotations\nfrom pendulum import parse\nfrom remote_tile_delivery import dag\n\ndef run_batch(batch_id: str, family: str, manifest_file: str, logical_date: str) -> None:\n    dag.test(execution_date=parse(logical_date), run_conf={\"batch_id\": batch_id, \"family\": family, \"manifest_file\": manifest_file})\n"""
    (tmp / "tools").mkdir()
    (tmp / "tools/run_batch.py").write_text(tool, encoding="utf-8")
    (tmp / "README.md").write_text("# 遥感切片分片续跑材料\n\ndags目录保存候选DAG，tools目录保存批次运行入口，results目录保存分片处理账、批次摘要和DAG结构，checkpoints目录保存可复用分片状态，release-summary.json供值班人员安排维护窗。\n", encoding="utf-8")
    if airflow_home.exists(): shutil.rmtree(airflow_home)
    if (tmp / "dags/events").exists(): shutil.rmtree(tmp / "dags/events")
    if (tmp / "dags/summaries").exists(): shutil.rmtree(tmp / "dags/summaries")
    for cache in tmp.rglob("__pycache__"): shutil.rmtree(cache)
    if OUTPUT.exists(): shutil.rmtree(OUTPUT)
    tmp.rename(OUTPUT)


if __name__ == "__main__":
    main()
