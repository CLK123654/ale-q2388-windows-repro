from __future__ import annotations

import json
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime


INPUT_ROOT = Path(__import__("os").environ.get("ALE_INPUT_ROOT", "/opt/remote-tile/input_data"))


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


@dag(
    dag_id="remote_tile_delivery",
    schedule=None,
    start_date=datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["remote-delivery", "dynamic-mapping"],
)
def remote_tile_delivery():
    @task
    def discover_shards() -> list[dict]:
        conf = get_current_context()["dag_run"].conf
        manifest = INPUT_ROOT / conf["manifest_file"]
        import csv

        with manifest.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        indexes = [int(row["map_index"]) for row in rows]
        if indexes != list(range(len(rows))):
            raise AirflowFailException("map_index不连续")
        return [
            {
                **row,
                "map_index": int(row["map_index"]),
                "record_count": int(row["record_count"]),
                "expected_count": int(row["expected_count"]),
                "batch_id": conf["batch_id"],
                "family": conf["family"],
            }
            for row in rows
        ]

    @task
    def process_shard(shard: dict) -> dict:
        signature = "|".join(
            str(shard[field])
            for field in ["shard_id", "record_count", "expected_count", "quality_code", "source_revision"]
        )
        checkpoint = Path("checkpoints") / shard["family"] / f'{shard["shard_id"]}.json'
        event = Path("events") / shard["batch_id"] / f'{shard["map_index"]:03d}.json'
        if shard["quality_code"] != "OK" or shard["record_count"] != shard["expected_count"]:
            value = {"batch_id": shard["batch_id"], "family": shard["family"], "map_index": shard["map_index"], "shard_id": shard["shard_id"], "action": "REJECTED", "outcome": "FAILED", "reason": "SHARD_CONTRACT", "signature": signature}
            _write_json(event, value)
            return value
        action = "PROCESSED"
        if checkpoint.exists() and json.loads(checkpoint.read_text(encoding="utf-8")).get("signature") == signature:
            action = "REUSED"
        if action == "PROCESSED":
            _write_json(checkpoint, {"family": shard["family"], "shard_id": shard["shard_id"], "map_index": shard["map_index"], "signature": signature, "status": "READY"})
        value = {"batch_id": shard["batch_id"], "family": shard["family"], "map_index": shard["map_index"], "shard_id": shard["shard_id"], "action": action, "outcome": "SUCCESS", "reason": "", "signature": signature}
        _write_json(event, value)
        return value

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_batch(shards: list[dict]) -> dict:
        conf = get_current_context()["dag_run"].conf
        directory = Path("events") / conf["batch_id"]
        events = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(directory.glob("*.json"))] if directory.exists() else []
        rejected = sum(row["action"] == "REJECTED" for row in events)
        checkpoint_count = sum((Path("checkpoints") / conf["family"] / f'{row["shard_id"]}.json').exists() for row in shards)
        value = {"batch_id": conf["batch_id"], "family": conf["family"], "discovered_count": len(shards), "processed_count": sum(row["action"] == "PROCESSED" for row in events), "reused_count": sum(row["action"] == "REUSED" for row in events), "rejected_count": rejected, "checkpoint_count": checkpoint_count, "release_status": "READY" if rejected == 0 else "HOLD"}
        _write_json(Path("summaries") / f'{conf["batch_id"]}.json', value)
        return value

    discovered = discover_shards()
    mapped = process_shard.expand(shard=discovered)
    summary = summarize_batch(discovered)
    mapped >> summary


dag = remote_tile_delivery()
