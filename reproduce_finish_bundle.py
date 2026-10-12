"""Run a real Beam/Dask pipeline and print the outputs gathered from workers."""
import argparse
import json
import platform

import apache_beam as beam
import dask
import distributed
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dask.dask_runner import DaskRunner
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue
from distributed import LocalCluster


class EmitAtFinish(beam.DoFn):
    def start_bundle(self):
        self.total = 0

    def process(self, element):
        self.total += element
        yield ("process", element)

    def finish_bundle(self):
        yield WindowedValue(("finish", self.total), 0, (GlobalWindow(),))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="before")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    expected = [["process", 1], ["process", 2], ["process", 3], ["finish", 6]]
    with LocalCluster(
        n_workers=2,
        threads_per_worker=2,
        processes=True,
        memory_limit="1GiB",
        dashboard_address=None,
        host="127.0.0.1",
    ) as cluster:
        options = PipelineOptions(
            ["--dask_client_address", cluster.scheduler_address,
             "--dask_npartitions", "1"]
        )
        p = beam.Pipeline(runner=DaskRunner(), options=options)
        _ = p | beam.Create([1, 2, 3]) | beam.ParDo(EmitAtFinish())
        result = p.run()
        try:
            state = result.wait_until_finish()
            partitions = result.client.gather(result.futures)
            actual = [list(value) for partition in partitions for value in partition]
            worker_info = result.client.scheduler_info()["workers"]
            info = {
                "label": args.label,
                "os": platform.platform(),
                "python": platform.python_version(),
                "beam": beam.__version__,
                "beam_source": beam.__file__,
                "dask": dask.__version__,
                "distributed": distributed.__version__,
                "worker_processes": len(worker_info),
                "runner_state": state,
                "expected": expected,
                "actual": actual,
                "finish_bundle_output_present": ["finish", 6] in actual,
                "matches_expected": actual == expected,
            }
        finally:
            result.client.close()
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(info, output, ensure_ascii=False, indent=2)
    print(json.dumps(info, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
