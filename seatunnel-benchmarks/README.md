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

## Run the checkpoint scheduling benchmark

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar CheckpointSchedulingBenchmark
```

`CheckpointSchedulingBenchmark` measures how long a due checkpoint trigger waits before its
scheduling thread runs it, as a distribution. This is separate from
`CheckpointingTimeBenchmark`, which measures checkpoint completion time: completion time is
dominated by the barrier round-trip and is close to blind to how the trigger was scheduled.

Parameters:

- `pipelineNum`: pipelines scheduling checkpoints on the member. Each `CheckpointCoordinator`
  builds its own two-thread pool, so this also sets the scheduler thread count.
- `checkpointIntervalMillis`: interval of the background trigger load. The default is far below
  the production default of 300000 so that a short iteration sees a realistic number of triggers.
- `triggerBodyMicros`: how long a trigger occupies its scheduling thread. It is a parameter, not a
  fixed cost, because the answer differs per deployment and it is what decides whether a fixed
  pool width is wide enough.

Sweep the pipeline count to see the axis that separates scheduling models:

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar CheckpointSchedulingBenchmark \
  -p pipelineNum=1,10,100,500 \
  -rf json \
  -rff seatunnel-benchmarks/target/checkpoint-scheduling-result.json
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
