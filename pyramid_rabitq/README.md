# Pyramid RaBitQ

Pyramid-style sampled-HNSW partitioning with per-partition RaBitQ-IVF indexes.
All search computation runs on interchangeable read nodes. Shared storage only
contains immutable index versions and exact vectors; there is no leaf service.

## Component contracts

- `HNSWRouter`: builds the sampled navigation graph, exposes its real Faiss
  level-0 adjacency, assigns corpus vectors, and ranks query partitions.
- `KaFFPaGraphPartitioner`: writes the weighted HNSW level-0 graph in METIS
  format and invokes KaHIP's KaFFPa executable. Many bottom-layer HNSW nodes
  are assigned to each storage partition. A recorded development fallback is
  available; production configuration requires KaFFPa explicitly.
- `RaBitQIVFIndex`: trains local IVF centroids, encodes centered residuals with
  a randomized orthogonal transform and one bit per dimension, stores codes in
  32-lane FastScan blocks, and returns candidates plus exact-vector locators.
- `PyramidBuilder`: performs immutable, streaming builds. Full partitions are
  never loaded into memory during RaBitQ construction.
- `ReadNodeEngine`: routes locally, scans selected RaBitQ indexes, reads exact
  vector blocks through a byte-bounded cache, deduplicates, and reranks.
- HTTP API: `GET /health`, `GET /index`, JSON `POST /search`, binary float32
  `POST /search/binary`, and authenticated `POST /admin/reload`.

RaBitQ packed-code scanning uses a GIL-free C++ extension. FastScan transposes
codes to `(block, packed_byte, lane)`, splits query LUTs into 4-bit tables, and
scans 16 lanes with AVX-512 or 8 lanes with AVX2. It does not quantize the
float LUT, so this optimization does not add approximation error. A NumPy
fallback remains available. `/health` exposes `native_scan`, `fastscan`,
`fastscan_backend`, and `code_layouts`; set `require_fastscan: true` to reject
an incompatible binary or old row-major index at service startup.

The normal query modes are approximate because HNSW partition selection and
IVF probing can exclude a true neighbor. Returned candidates are always
reranked using the stored float32 vectors. Searching every partition and every
IVF list with a candidate budget covering all rows is the exact baseline.

## Routing and IVF sizing defaults

The builder samples at least `max(hnsw.sample_size, corpus_rows *
hnsw.sample_ratio)` vectors, capped by the corpus size. The default ratio is
20%. KaFFPa partitions the weighted level-0 sample graph; one graph partition
becomes one HDF5/RaBitQ storage partition.

`ivf.target_vectors_per_list` defaults to 384 and must be between 256 and 512.
The builder derives a separate `nlist` from each storage partition's actual
row count, keeping mean occupancy in that range whenever the partition has at
least 256 rows. KMeans can produce imbalanced individual posting lists, so the
manifest records every partition's resolved `ivf_nlist` for inspection.

Routing and IVF probing are separate controls: `hnsw.route_partitions=2`
selects the two closest graph partitions, while `ivf.nprobe=2` scans the two
closest IVF posting lists inside each selected partition. The online candidate
budget is `top_k * candidate_multiplier`, with the multiplier reduced to 8.

KaFFPa is only needed while building an index, not on read nodes. The rootless
installer pins upstream KaHIP v3.25 commit
`2e86d3da2065bcf6ddbf6fb84d2b52f33db1061f`, verifies the source archive, and
builds only the serial KaFFPa target without MPI:

```bash
source ../../route.sh                    # if this cluster requires the proxy
NCORES=8 ./tools/install_kaffpa.sh
export PYRAMID_KAFFPA_EXECUTABLE="$PWD/.local/kahip-v3.25/bin/kaffpa"
```

The prefix can be overridden with `./tools/install_kaffpa.sh /path/to/prefix`.
The installer records source commit, source SHA256, and binary SHA256; these
are copied into every newly built index manifest. With
`hnsw.require_kaffpa: true`, a missing or failed binary aborts the build. The
manifest fields `graph_partition_backend` and
`graph_partition_fallback_reason` prevent an unnoticed fallback.

## Local development

```bash
cd gene_database/pyramid_rabitq
python -m pip install -e .
python setup.py build_ext --inplace
PYTHONPATH=src python -m unittest discover -s tests -v
```

Inspect and build a small source directory:

```bash
pyramid-rabitq inspect --source ../tmp_emb_opt --dimension 2560
pyramid-rabitq build \
  --source ../tmp_emb_opt \
  --index-root /path/to/index-root \
  --version v000001 \
  --config config/build.example.yaml

pyramid-rabitq validate \
  --source ../tmp_emb_opt \
  --index-root /path/to/index-root \
  --queries 100 --top-k 10 --output recall.json
```

Each version is immutable. A successful build atomically updates the text file
`INDEX_ROOT/CURRENT`. Read nodes only reload after the complete manifest and
all shard indexes exist.

New builds use FastScan automatically. To migrate an existing immutable v1
version, copy and convert it without touching the published version, then
activate it atomically:

```bash
PYTHONPATH=src python tools/convert_index_fastscan.py \
  --index-root /path/to/index-root \
  --source-version v000001 \
  --target-version v000001-fastscan \
  --activate
```

Conversion keeps centroids, IDs, exact vectors, and routing unchanged. Only
the packed-code layout changes; the old version remains available for rollback.

## VectorDBBench evaluation

`benchmarks/vectordbbench_adapter.py` implements VectorDBBench's `VectorDB`
contract over the binary HTTP endpoint. The current integration evaluates a
prebuilt immutable index with the official serial and multiprocessing search
runners. It intentionally does not report VectorDBBench load duration because
the service has no online insert API.

With the official VectorDBBench source in `third_party/VectorDBBench` and its
dependencies in `.deps/vectordbbench-clean`, run:

```bash
IR_DATASETS_HOME=/tmp/vectordb_bench/ir_datasets \
IR_DATASETS_TMP=/tmp/vectordb_bench/ir_datasets_tmp \
PYTHONPATH=.deps/vectordbbench-clean:third_party/VectorDBBench:src:. \
python tools/run_vectordbbench.py \
  --index-root /path/to/index-root \
  --wheel dist/release/wheelhouse/pyramid_rabitq-0.6.0-cp312-cp312-linux_x86_64.whl \
  --corpus-h5 /path/to/corpus.h5 \
  --query-h5 /path/to/queries.h5 \
  --output /path/to/results
```

Defaults follow the VectorDBBench search profile: Top-100, 30 seconds per
concurrency, and concurrency levels 1, 5, 10, 20, 30, 40, 60, and 80. The
runner computes exact Faiss ground truth from the complete supplied corpus.

## VecBench paper evaluation

The distinct paper benchmark
[`ruc-datalab/VecBench`](https://github.com/ruc-datalab/VecBench), associated
with DOI `10.1145/3802125`, is integrated through
`benchmarks/vecbench_adapter.py`. The read API and adapter support VecBench's
`equal`, `range`, and `tags contains` predicates. Filtered queries use scalar
metadata to safely prune partitions that cannot match. Equality/range columns
have sorted row indexes and tags use an inverted index. Very small qualifying
sets are scanned exactly; otherwise `nprobe` grows from its configured value
only until the selected IVF lists contain the requested rerank candidate
budget.

For the public YFCC reproduction, prepare the 10M × 1920 data and run the
official VecBench serial/concurrent executors with:

```bash
PYTHONPATH=src:. python tools/prepare_vecbench_yfcc.py \
  --vectors /path/to/base.10M.u8bin \
  --metadata /path/to/base.metadata.10M.spmat \
  --query-file third_party/VecBench-paper/config/YFCC/E2E_queries.yaml \
  --output /path/to/yfcc-10m

PYTHONPATH=src:. python tools/run_vecbench_yfcc.py \
  --index-root /path/to/yfcc-index \
  --corpus-h5 /path/to/yfcc-10m/shard_0.h5 \
  --query-yaml third_party/VecBench-paper/config/YFCC/E2E_queries.yaml \
  --output /path/to/results \
  --concurrency 1 10 50
```

The complete six-phase integration is registered directly in the pinned
official checkout at `third_party/VecBench-paper/databases/pyramid.py`. It
implements the official `BaseVD` methods plus the `update` and
`delete_by_ids` hooks used by P5/P6. Run the official executor itself from its
working directory:

```bash
cd third_party/VecBench-paper
PYTHONPATH=../..:../../src:. \
PYRAMID_VECBENCH_SOURCE=/absolute/path/to/source-h5-directory \
PYRAMID_VECBENCH_INDEX=/absolute/path/to/index-root \
PYRAMID_VECBENCH_BUILD_CONFIG=/absolute/path/to/config/build.vecbench-yfcc.yaml \
PYRAMID_ADMIN_TOKEN=secret \
python benchmark.py \
  --case test --database pyramid --dataset YFCC --scale 10000000 \
  --algorithm pyramid --concurrency 50 --times 1 \
  --phases P1 P2 P3 P4 P5 P6 \
  --query_file config/YFCC/E2E_queries.yaml \
  --ground_truth data/YFCC/ground_truth/E2E_192_10M.json \
  --output pyramid-yfcc10m.json

cd ../..
python tools/score_vecbench_official.py \
  third_party/VecBench-paper/pyramid-yfcc10m.json
```

P4/P5/P6 update the source HDF5 corpus, rebuild an immutable Pyramid index
version, and reload the read node. Their timings include the full rebuild.
`tools/export_vecbench_incremental.py` emits the exact binary files consumed
by the official incremental helpers.

The scorer ranks all seven published metrics against the official YFCC 10M
table and reproduces the website formula:
`Vrank = sum(rank(T0), rank(Recall), rank(Latency), rank(QPS), rank(T1),
rank(T2), rank(T3)) / 6`. It refuses to label a non-10M run official unless
`--allow-nonstandard` is explicitly supplied.

Reproducibility limitation: the public VecBench repository does not contain
`equal_cluster.u2bin` or its YFCC ground truth, and its `range.f4bin` is empty.
The preparation tool therefore uses deterministic, provenance-recorded
surrogates for those two columns; official vectors, tags, and queries are kept.
Results using reconstructed scalar files are labelled
**reconstructed-public**, not official leaderboard Vrank. Dynamic timings are
only comparable when the same machine, YFCC 10M inputs, concurrency, and
incremental ratio are used.

## Immutable index updates

Reads use the base version named by `INDEX_ROOT/CURRENT`. To publish changes,
rebuild from the source corpus and call authenticated `POST /admin/reload`.
Each query retains the index references acquired at its start.

The LSM implementation and its mutation/snapshot APIs have been removed.
Existing `INDEX_ROOT/lsm` data is retained on disk but is not read or migrated;
changes present only in that layer are absent from search results. Historical
LSM benchmark results do not describe the current rebuild-based update path.
This is a targeted LSM rollback retaining scalar filtering, not a byte-for-byte
restoration of the older v0.5.0 package.

## Query API

```bash
curl -X POST http://read-node:8080/search \
  -H 'content-type: application/json' \
  -d '{"vector":[0.1, 0.2], "top_k":10}'
```

Binary requests avoid parsing 2,560 JSON numbers. The body is little-endian
contiguous float32 and query options are headers:

```bash
curl -X POST http://read-node:8080/search/binary \
  -H 'content-type: application/octet-stream' \
  -H 'x-top-k: 10' \
  --data-binary @query.f32
```

The response contains exact reranked distances, global IDs, selected storage
partitions, index version, and candidate count. Global IDs use
`(source_shard_id << 32) | source_row`.

## Rootless SSH deployment

Build the release on a machine with package-index access:

```bash
./tools/build_release.sh
```

Copy `dist/pyramid-rabitq-node.tar.gz` and the index root to every read node.
On each node, without sudo or Docker:

```bash
mkdir -p release && tar -xzf pyramid-rabitq-node.tar.gz -C release
release/deploy/install_node.sh release "$PWD/pyramid-node" --shared-env
cp "$PWD/pyramid-node/service.yaml.example" "$PWD/pyramid-node/service.yaml"
# Edit service.yaml: index_root must be a shared mount or node-local index copy.
"$PWD/pyramid-node/start_read_node.sh"
"$PWD/pyramid-node/health_check.sh"
```

`--offline` requires dependency wheels produced by
`./tools/build_release.sh dist/release --with-deps`. If cluster nodes expose
the same preinstalled Python packages as the build host, the app-only archive
can be installed with `--shared-env`; the installer performs an import check
and fails immediately if a dependency is absent. Without either condition,
omit the mode to let pip use the configured package index.

Set `PYRAMID_ADMIN_TOKEN` before starting if remote index reload is required.
Put the node addresses behind an existing user-space proxy, scheduler load
balancer, or client-side round-robin. One service process is recommended per
NUMA allocation; start multiple installation directories/ports when desired.

For several SSH nodes, put one hostname per line in `hosts.txt`, prepare one
service YAML whose `index_root` is visible at the same path on all nodes, then:

```bash
release/deploy/deploy_cluster.sh \
  pyramid-rabitq-node.tar.gz hosts.txt /shared/user/pyramid-node service.yaml
```

The deployment helper installs files but deliberately does not start services;
this leaves port allocation and scheduler/NUMA binding under operator control.

## Operational constraints

- `tmp_emb_large` currently contains FASTA/checkpoint files, not HDF5 vectors.
- Install the pinned KaHIP/KaFFPa build with `tools/install_kaffpa.sh` on the
  offline index-builder host. Read-node hosts do not need the binary.
- `require_kaffpa` defaults to true. Set `partition_backend: fallback`
  explicitly only for development tests.
- Do not point readers at a version directory directly; use `CURRENT` so that
  HNSW labels and RaBitQ shards cannot be mixed across versions.
- `partition_workers` bounds one process's shared partition executor;
  `max_inflight_queries` prevents nested request/partition oversubscription.
- Start scripts default OpenMP, OpenBLAS, MKL and NumExpr to one thread. Tune
  these only together with process count and NUMA binding.
- `mmap_indexes: false` improves hot-index throughput when all packed codes fit
  comfortably in RAM. Full-scale deployments should normally retain mmap.
- AVX-512 is the preferred FastScan backend. Check `fastscan_backend` on every
  node because heterogeneous CPU generations can have different throughput.
