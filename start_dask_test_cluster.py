"""A bounded local cluster shared by the actual Beam runner test suite."""
import json
import os
import time
from pathlib import Path

from distributed import Client, LocalCluster


def main():
    evidence = Path(__file__).parent / "evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    with LocalCluster(n_workers=2, threads_per_worker=2, processes=True,
                      memory_limit="1GiB", dashboard_address=None,
                      host="127.0.0.1") as cluster:
        with Client(cluster) as client:
            client.wait_for_workers(2)
            info = {"address": cluster.scheduler_address,
                    "workers": len(client.scheduler_info()["workers"]),
                    "pid": os.getpid()}
            (evidence / "test-cluster.json").write_text(json.dumps(info))
            print(json.dumps(info), flush=True)
            while not (evidence / "stop-test-cluster").exists():
                time.sleep(1)


if __name__ == "__main__":
    main()
