# GitHub publication validation — 2026-10-20

The current DHDGS source passed all 388 tests in 58.105 seconds.
See `regression.log` for the complete test results.

```sh
PYTHONPATH=gene_database/DHDGS/python DHDGS_TEST_URING=1 \
  gene_database/DHDGS/.venv/bin/python -m unittest discover \
  -s gene_database/DHDGS/tests -v
```

Tests require local sockets and multiprocessing; the restricted sandbox cannot
run the network integration tests. The successful run used the host environment.

Publication includes current source, YAML configuration, deployment tools,
unit/integration tests, experiment scripts, design documents, and compact
reports and plots. Datasets, indexes, compiled artifacts, credentials, raw
query logs, runtime state dumps, and experiment source snapshots remain local.
Historical reports retain their original measurements and configuration; they
are not new performance measurements of this publication. Publishing does not
restart or redeploy the running cluster.
