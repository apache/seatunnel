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

Write JSON results:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelRowBenchmark \
  -rf json \
  -rff seatunnel-benchmarks/target/benchmark-result.json
```

## Run ProtoStuff serialization microbenchmarks

`ProtoStuffSerializerBenchmark` measures one `IMapFileData` envelope per operation, with a
pre-serialized Long key and a fixed 1,024-character ASCII String value. It does not measure nested
key/value conversion, disk I/O, Hazelcast, or cluster startup. Setup prepares the input and warms
the schema cache; the measured calls retain normal serializer allocations.

The envelope is not a `SerializerDeserializerWrapper`: both measured methods call `getSchema`
on every invocation. Eight threads are used by default to expose contention on the shared cache.
Scores are throughput in `ops/ms` (higher is better), with a fixed 4 GiB heap, G1, pre-touch,
disabled explicit GC, and four JVM-visible processors, matching the pipeline benchmark's JVM limits.

Run both methods with the default eight threads (the static schema cache is shared):

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar ProtoStuffSerializerBenchmark \
  -rf json -rff seatunnel-benchmarks/target/protostuff.json
```

For allocation or lock diagnostics, select one method with the existing profiling script:

```bash
bash tools/benchmarks/profile_benchmarks.sh profile gc \
  --benchmark 'ProtoStuffSerializerBenchmark.deserializeWalRecord$' -- -t 8
bash tools/benchmarks/profile_benchmarks.sh profile lock \
  --benchmark 'ProtoStuffSerializerBenchmark.deserializeWalRecord$' -- -t 8
```

Compare revisions using the same JDK, hardware, thread count, and JMH settings. Multi-thread
throughput is the total across threads. Run performance comparisons without profilers and collect
profiles separately. This is a warm-cache microbenchmark, not a cold schema
initialization or end-to-end WAL recovery benchmark. The Benchmarks workflow also exposes this
class; it is not added to the default `benchmarks_core` suite.

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
