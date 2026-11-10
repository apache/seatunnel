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

Run the `MultiTableSink` record handoff benchmark without starting an engine or database:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar MultiTableSinkBenchmark -p replicaNum=1,5,15
```

The score measures bounded record handoff throughput with per-writer sequence checks; final
draining and count verification run outside the timed iteration.

Write JSON results:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelRowBenchmark \
  -rf json \
  -rff seatunnel-benchmarks/target/benchmark-result.json
```

Run the JDBC row converter benchmark with an in-memory result set and parameter recorder.

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar JdbcRowConverterBenchmark \
  -p columns=20 -p typeProfile=TEMPORAL -w 500ms -r 500ms -prof gc
```

The benchmark measures conversion of the current result-set row and binding of a prepared
statement as separate operations. The fixture validates the decoded row and bound parameters
before measurement. Each type profile repeats a five-column pattern to keep the mix fixed at
5, 10, 15, and 20 columns.

| Type profile | Five-column pattern |
| --- | --- |
| `INTEGRAL` | Boolean, tinyint, smallint, integer, bigint |
| `DECIMAL_AND_BINARY` | Float, double, decimal, varchar, varbinary |
| `TEMPORAL` | Date, time, timestamp, timestamp with time zone, null varchar |

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
