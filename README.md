# Apache Beam DaskRunner finish-bundle validation

Evidence for [apache/beam#40519](https://github.com/apache/beam/issues/40519), captured on an Ubuntu x86_64 host on 2026-10-12. Baseline: `d71149317ebf46376968e16367d8c4f8f6e5b0ea`.

The actual pipeline ran on **two local Dask worker processes**, with one input partition. On the original source it reported `DONE` but returned only three process records. With the fix it also returned `['finish', 6]`, matching the complete expected output. The JSON files were gathered from the runner's actual futures.

## Screenshots and results

- `beam-before.jpg`: captured original-code execution results and failed regressions.
- `beam-after.jpg`: captured before/after output comparison, passed tests, and dependency validation.
- `before.json` and `after.json`: actual worker outputs and environment information.
- `dask-red.log` / `dask-red.xml`: both new tests fail on the original code because output records are missing.
- `dask-green.log` / `dask-green.xml`: full DaskRunner directory, **29 passed, 1 existing xfailed, 5 subtests passed**.
- The existing expected failure is `test_pardo_windowed_side_inputs`; this change does not alter the runner's windowed-side-input support.
- `pip-check.log`: `No broken requirements found.`
- `dependencies.txt`: the actual installed environment.

The screenshots are of a local, read-only evidence viewer displaying captured backend results. They are not screenshots of a Dask product UI. No simulation or fabricated result is used.

## Dependency installation actually used

The validation used the pure-Python SDK from an editable checkout; Cython extensions were not compiled. The evaluator changed by this patch is Python code. No new project runtime dependency was introduced.

With an available Python 3.12 interpreter, from a checkout containing both `sdks/python` and `model`:

```bash
python3.12 -m venv /tmp/beam-dask-venv
source /tmp/beam-dask-venv/bin/activate
python -m pip install setuptools 'wheel>=0.36.0' \
  'grpcio-tools==1.62.1' 'mypy-protobuf==3.5.0' 'distlib==0.4.3' \
  'numpy>=1.14.3,<2.5.0' 'pyyaml>=3.12,<7' 'jinja2>=2.7.1,<4' \
  'yapf==0.43.0' 'ruff==0.15.22'
python -m pip install --no-build-isolation -e './sdks/python[dask,test,dataframe]'
# grpcio-tools is only needed for source generation. Its Python 3.12 build
# pin requires protobuf 4, whereas the installed 2026 runtime dependencies
# require protobuf 6. Remove the build-only tool after generation.
python -m pip uninstall --yes grpcio-tools
python -m pip install 'protobuf>=6.33.5,<7'
python -m pip check
```

Actual key versions: Python 3.12.14, Beam 2.78.0.dev0, Dask/distributed 2026.8.0, protobuf 6.33.6, pyarrow 25.0.1, pandas 2.3.3, pytest 9.1.1.

`install-beam-activated.log` preserves the source installation log, including the temporary build/runtime protobuf conflict; `install-runtime-protobuf.log` and `pip-check.log` record its resolution. `dependencies.txt` records the final consistent environment. An isolated PEP 517 build is another way to keep build-only dependencies separate.

## Reproduce and verify

Run `reproduce_finish_bundle.py --label after --output after.json` in the installed SDK environment. It starts and closes its own two-worker local cluster.

For the full DaskRunner test directory, run `start_dask_test_cluster.py` in a separate terminal. Use the scheduler address printed by that script:

```bash
cd <beam-checkout>/sdks/python
python -m pytest apache_beam/runners/dask/ \
  --test-pipeline-options='--dask_client_address=<printed-address> --dask_npartitions=1' \
  --timeout=90 -q
ruff check apache_beam/runners/dask/dask_runner_test.py \
  apache_beam/runners/dask/transform_evaluator.py
yapf --diff apache_beam/runners/dask/dask_runner_test.py \
  apache_beam/runners/dask/transform_evaluator.py
```

Restart the cluster after editing an installed source module: long-lived workers cache imported Python modules. The recorded final green suite used fresh workers, and the standalone before/after executions each used freshly started workers. Stop the test cluster by creating `evidence/stop-test-cluster` beside the helper script; remove that marker before starting it again.

Validation is limited to local Python/Dask execution. Java, cloud runners, the full multi-language Beam repository test suite, and Cython extensions were not tested.
