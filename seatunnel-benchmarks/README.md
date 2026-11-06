# SeaTunnel Benchmarks

This module contains isolated micro and full-pipeline benchmarks for Apache SeaTunnel.

The benchmark module is intentionally excluded from the default Maven reactor. It is only enabled
through the `benchmark` profile, so benchmark-only dependencies such as JMH do not affect
SeaTunnel's normal build, release, or runtime classpath.

The build creates two independent artifacts:

- `target/benchmarks.jar`: shaded JMH runner for JVM micro benchmarks.
- `target/seatunnel-benchmark-plugins.jar`: lightweight Source, Transform, and Sink plugins for
  Zeta full-pipeline benchmarks.

## Run micro benchmarks

Build the benchmark module and its dependencies:

```bash
./mvnw -Pbenchmark -pl seatunnel-benchmarks -am -DskipTests package
```

Quickly verify that the runner and `SeaTunnelRow` benchmark work:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar \
  'SeaTunnelRowBenchmark.copyPlainRow' \
  -f 1 -wi 1 -i 1 -w 1s -r 1s
```

Run all benchmarks with their configured forks and iterations:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar \
  'org.apache.seatunnel.benchmark.*'
```

Run only the `SeaTunnelRow` benchmarks:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelRowBenchmark
```

Run only the `ProtoStuffSerializer` benchmarks:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar ProtoStuffSerializerBenchmark
```

Run the production MultiTableSink handoff path without starting Zeta, Hazelcast members, or a
database:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar \
  'MultiTableSinkBenchmark.recordHandoff$' \
  -p replicaNum=1,5,15 -prof gc \
  -rf json -rff seatunnel-benchmarks/target/multisink-handoff.json
```

The fixture creates `MultiTableSink` and its real asynchronous writers around in-memory sinks.
The default is one logical table, hash routing, no simulated delay, and one JMH producer. Override
`tableCount=4` for multiple tables or `routing=SINGLE_QUEUE` as a control that keeps all keys on
replica zero. `consumerBatchSize` (default 1000) and `batchDelayMicros` (default 0) model consumer
backpressure without external I/O. They do not batch the production queue handoff.

Rows are preallocated and read-only. At each iteration boundary the fixture drains queued and
in-flight writes, flushes tail batches, and verifies each writer's count and order-sensitive
checksum. Draining and verification are outside the timed operation. The score is steady-state
**bounded admission/handoff throughput**, not a JDBC durability or checkpoint-latency measurement;
up to the production queue capacity plus in-flight work can remain at the measurement boundary.
Simulated consumer tail batches are also flushed outside timing and may contain fewer than
`consumerBatchSize` rows per table/replica writer.
Use sufficiently long iterations to make that bounded tail small relative to measured work.

The default `benchmarks_core` suite includes this regression, and the workflow also exposes
`MultiTableSinkBenchmark` as a standalone choice. Baseline/candidate comparisons of this method
require the fixture on both revisions.

Write JSON results:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelRowBenchmark \
  -rf json \
  -rff seatunnel-benchmarks/target/benchmark-result.json
```

## Run Zeta full-pipeline benchmarks

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelPipelineBenchmark
```

## Run Zeta storage benchmarks

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar CheckpointStorageBenchmark
java -jar seatunnel-benchmarks/target/benchmarks.jar IMapJobStorageBenchmark
java -jar seatunnel-benchmarks/target/benchmarks.jar IMapDagStorageBenchmark
java -jar seatunnel-benchmarks/target/benchmarks.jar IMapWalStorageBenchmark
```

## Install async-profiler

CPU, wall-clock, and lock profiling require async-profiler's library and bundled `jfrconv`.
Set `ASYNC_PROFILER_HOME` to the complete installation. On macOS with Homebrew:

```bash
brew install async-profiler
export ASYNC_PROFILER_HOME="$(brew --prefix async-profiler)"
```

On Linux x64:

```bash
SEATUNNEL_ASYNC_PROFILER_VERSION=4.5
SEATUNNEL_ASYNC_PROFILER_HOME="${PWD}/seatunnel-benchmarks/target/async-profiler"
SEATUNNEL_ASYNC_PROFILER_ARCHIVE="${SEATUNNEL_ASYNC_PROFILER_HOME}.tar.gz"

curl --fail --location --retry 5 --retry-all-errors \
  --output "${SEATUNNEL_ASYNC_PROFILER_ARCHIVE}" \
  "https://github.com/async-profiler/async-profiler/releases/download/v${SEATUNNEL_ASYNC_PROFILER_VERSION}/async-profiler-${SEATUNNEL_ASYNC_PROFILER_VERSION}-linux-x64.tar.gz"
echo "89546fbb9ee0fc5496c7edd4099b0709489bc78b0d8057ccbb4b801f6b032b62  ${SEATUNNEL_ASYNC_PROFILER_ARCHIVE}" \
  | sha256sum --check --strict
mkdir -p "${SEATUNNEL_ASYNC_PROFILER_HOME}"
tar --extract --gzip \
  --file "${SEATUNNEL_ASYNC_PROFILER_ARCHIVE}" \
  --directory "${SEATUNNEL_ASYNC_PROFILER_HOME}" \
  --strip-components=1
export ASYNC_PROFILER_HOME="${SEATUNNEL_ASYNC_PROFILER_HOME}"
```

## Profile one benchmark

```bash
bash tools/benchmarks/profile_benchmarks.sh profile cpu \
  --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$'

bash tools/benchmarks/profile_benchmarks.sh profile wall \
  --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$'

bash tools/benchmarks/profile_benchmarks.sh profile lock \
  --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$'

bash tools/benchmarks/profile_benchmarks.sh profile gc \
  --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$'

bash tools/benchmarks/profile_benchmarks.sh capture jfr \
  --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$'
```

GC and JFR use JMH's built-in profilers and do not require async-profiler. Override JMH warmup and
measurement settings after `--`:

```bash
bash tools/benchmarks/profile_benchmarks.sh profile gc \
  --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$' \
  -- -wi 1 -i 1 -w 1s -r 1s
```

The profiling script accepts exactly one benchmark method and always uses one fork.

See the [Zeta benchmark guide](../docs/en/engines/zeta/benchmark.md) for workflow reports, IntelliJ
IDEA setup, benchmark parameters, metrics, result interpretation, and contribution guidance.
