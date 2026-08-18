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

## Benchmark reports

The `Benchmarks` GitHub Actions workflow builds three artifacts for every Java version:

- the original JMH JSON, which preserves all forks and iteration samples;
- a versioned `*.report.json`, which normalizes benchmark name, parameters, score, error, unit,
  direction, commit, JVM, CPU, and runner image;
- a Markdown report, which is also displayed in the GitHub Actions job summary.

The normalized report also includes median pipeline throughput, latency P50/P95/P99/max, latency
growth, completeness, and the number of sustainable samples. Keeping the raw samples and a schema
version allows a later Codespeed service or regression checker to consume saved artifacts without
parsing console logs. The workflow also saves the CPU, kernel, runner image, memory, and JDK
fingerprint. It does not push benchmark data to a repository branch.

Manual runs provide a `benchmarks` dropdown for common JMH selectors. The optional
`custom_benchmarks` input accepts any class name, method name, or regular expression and overrides
the dropdown, so a new benchmark can run before it is added to the common choices. `.*` runs every
current and future benchmark. An optional PR number compares that PR with `seatunnel_ref` on the
same worker in `base -> PR -> PR -> base` order. The comparison report uses the median of the two
baseline and two candidate runs and shows a direction-adjusted percentage; positive means the
candidate moved in the favorable direction.

JMH treats selectors as regular expressions. Append `$` to an exact method selector when other
method names share the same prefix.

GitHub-hosted runners can execute the workflow reliably while still having materially different
host CPU performance. Treat these artifacts as trend and functional-check data, not as a regression
gate based on one run or on JMH's within-run `scoreError`. A future regression gate should compare
the base and change on the same worker or use a fixed self-hosted runner.

## IntelliJ IDEA

The benchmark module is behind the inactive `benchmark` Maven profile, so IDEA may not import it
automatically after opening the SeaTunnel root project.

To make IDEA recognize the module:

1. Open the Maven tool window.
2. Expand `Profiles`.
3. Enable the `benchmark` profile.
4. Click `Reload All Maven Projects`.

If the module is still not shown, right-click `seatunnel-benchmarks/pom.xml` and choose
`Add as Maven Project`, then reload Maven once more.

## Interpreting results

Benchmark results are sensitive to machine load, JVM warmup, CPU frequency, and runner type. Prefer
comparing repeated baseline/change runs on the same machine instead of comparing absolute numbers
from different machines.

## Run Zeta full-pipeline benchmarks

```bash
java -jar seatunnel-benchmarks/target/benchmarks.jar SeaTunnelPipelineBenchmark
```

See the [Zeta benchmark guide](../docs/en/engines/zeta/benchmark.md) for architecture, parameters,
resource settings, and result interpretation.

## Adding benchmarks

Keep benchmark cases small and focused. Good first targets are hot paths that can run on a single
machine without external services, such as:

- `SeaTunnelRow` operations
- format parsing and serialization
- transform hot paths
- connector option parsing
- split generation logic

Common JMH and JVM settings live in the single `BenchmarkBase`. Individual benchmark classes
extend it and only define their own data setup and benchmark methods. Engine lifecycle and
engine-level controls belong in `SeaTunnelEnvironmentContext`, so checkpoint, failure-recovery,
and metrics scenarios can be added without duplicating cluster setup in each benchmark.
