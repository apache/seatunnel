---
title: Contribute Performance Improvements
---

# Contribute Performance Improvements

SeaTunnel welcomes contributions that solve real performance problems contributors have already
discovered. A benchmark is not a tool for finding problems; it verifies whether a problem comes
from a particular production path. A frequently called method is not necessarily a bottleneck.

```text
Discover an anomaly → form and test a suspicion → Benchmark PR → merge into dev → fix PR → compare
```

## From a Discovered Problem to a Bottleneck

Performance contributions normally begin with an anomaly a contributor has already encountered in
production, load testing, or incident analysis, such as OOM, checkpoint timeouts, saturated CPU,
throughput loss, latency growth, or stalled threads. This guide does not ask contributors to scan
metrics for something to optimize; it explains how to validate an already discovered problem.

```text
Discover an anomaly → suspect a production path → reproduce it under control → confirm or reject
```

| Stage | Decision basis |
|---|---|
| Discover an anomaly | The problem occurs under a defined workload and environment, with a describable effect on throughput, latency, resources, or job execution. |
| Form a suspicion | Logs, metrics, thread stacks, or profiles point to a production path and explain why that path may cause the problem. |
| Confirm or reject | A controlled experiment reproduces the original problem. Changing the path's cost changes the system-level symptom as predicted in repeatable runs. Otherwise, reject the path and continue the investigation. |

A benchmark reproduces and validates a path that is already suspected; it is not a tool for broadly
searching the codebase for possible hotspots. Frequent calls, a high CPU share, or lock samples do
not prove a bottleneck by themselves.

For example, after a checkpoint timeout is traced to state serialization, use controlled state data
to reproduce the serialization cost and verify whether reducing that cost also shortens the
checkpoint. If the microbenchmark improves but checkpoint duration does not, that path does not
explain the original problem.

## Build a Reproducible Experiment

Choose a workload that reaches the identified production path, then define the logical operation,
input shape, concurrency, and timed boundary. Keep fixture construction and validation outside
timing unless they are the subject of the test.

Validate the output so a fast but incomplete operation cannot produce a successful result. Repeat
the experiment under controlled conditions and confirm that it reproduces the problem. See
[Zeta Benchmark](../engines/zeta/benchmark.md) for local execution and profiling commands.

## Submit the Benchmark Separately

After the experiment identifies a reproducible bottleneck, open a focused Benchmark PR containing
the benchmark, deterministic fixtures, validation tests, and matching English and Chinese
documentation. Do not include the performance optimization in this PR.

After the Benchmark PR merges into `dev`, record its merge commit. That commit is the first valid
baseline for the new benchmark.

:::caution Both Revisions Must Use the Same Benchmark

If the benchmark exists only in the performance-fix PR, the baseline cannot run it. If the benchmark
or fixture changes between revisions, the result cannot isolate the production-code change.

:::

## Submit and Measure the Performance Fix

Create the performance-fix branch from a revision of `dev` that already contains the benchmark.
Keep the benchmark and its parameters unchanged while implementing the optimization.

Run the `Benchmarks` workflow with:

- `seatunnel_ref` set to the exact baseline commit;
- `pr_number` set to the performance-fix PR;
- the same benchmark method, parameters, and JDK for both revisions.

Use an unprofiled comparison to quantify improvement or regression. Profiling results may explain
the cause, but profiler overhead makes their Score unsuitable for the comparison.

## Share Reproducible Evidence

Include the following in the performance-fix PR:

- Baseline and Candidate commit SHAs;
- exact benchmark method and workload parameters;
- JDK, JVM settings, and relevant machine information;
- comparison report and raw JMH results;
- the measurement boundary and the conclusion it supports.

New benchmarks should run on demand first. Add one to a scheduled suite only when its workload is
representative, runtime is bounded, and repeated runs are stable enough to detect useful changes.
