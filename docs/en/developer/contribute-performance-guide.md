---
title: Contribute Performance Improvements
---

# Contribute Performance Improvements

SeaTunnel welcomes contributions that improve performance for representative workloads or fix
measured regressions. Performance work benefits from early, open discussion and evidence that
others can reproduce. A benchmark can reveal a regression or test a suspected cause, but a faster
microbenchmark alone does not establish a benefit to users.

```text
Observe a problem → discuss scope → reproduce → optimize → compare → review trade-offs
```

## Build Community Consensus

Before substantial implementation, open or reuse a [GitHub Issue](https://github.com/apache/seatunnel/issues)
and describe:

- the affected workload and SeaTunnel execution path;
- the observed effect on throughput, latency, resource use, or job stability;
- the environment and evidence used to observe the problem;
- the expected benefit and proposed scope.

The initial report does not need a finished benchmark. It should contain enough evidence for the
community to discuss whether the problem is relevant, whether the proposed experiment represents a
useful SeaTunnel workload, and whether the scope is appropriate. Use the
[dev mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org) when the change affects
multiple modules, introduces a lasting maintenance commitment, or needs a broader design decision.

A measured speedup contributes evidence to that discussion; it does not decide the outcome by
itself. The community also considers correctness, compatibility, other workloads, resource
trade-offs, implementation complexity, and maintenance cost.

## Build a Reproducible Benchmark

Choose a workload that represents the reported problem and reaches the affected production path.
Define the logical operation, input shape, concurrency, warmup, measurement duration, and timed
boundary. Explain how these choices relate to actual SeaTunnel workloads. Frequent calls, a high CPU
share, or lock samples do not prove a bottleneck by themselves.

Keep fixture construction and result validation outside the timed region unless they are the subject
of the test.

Validate the output so an incomplete operation cannot appear faster by doing less work. Run enough
repetitions to show normal variation, and report representative results rather than selecting the
best run. Test relevant input sizes and concurrency levels, including cases that may regress. Make
resource trade-offs explicit; for example, higher throughput obtained by using more memory may not
be an improvement for every workload.

See [Zeta Benchmark](../engines/zeta/benchmark.md) for local execution and profiling commands.

## Add a Benchmark

Reuse an existing benchmark when it represents the problem. A new benchmark should have:

- a workload that reaches the affected production path;
- deterministic fixtures and output validation;
- bounded runtime and results stable enough to detect a useful change.

For the current `Benchmarks` workflow, both revisions build their own benchmark module. A benchmark
that exists only in the optimization PR cannot run on the baseline revision. Propose a new benchmark
in a focused PR first so the community can review the workload and measurement independently. After
it merges into `dev`, create the optimization branch from a revision that contains it. Merging the
benchmark establishes a shared experiment; it does not predetermine the outcome of a later proposal.

:::caution Compare the Same Experiment

Baseline and Candidate must use the same benchmark code, fixtures, parameters, JDK, and measurement
boundary. If any of them differ, the result cannot isolate the production-code change.

:::

## Submit and Measure the Performance Fix

Keep the benchmark and its parameters unchanged while implementing the optimization. Run the
`Benchmarks` workflow with:

- `seatunnel_ref` set to the exact baseline commit;
- `pr_number` set to the performance-fix PR;
- the same benchmark method, parameters, and JDK for both revisions.

Use an unprofiled comparison to quantify improvement or regression. Profiling can help explain the
cause, but profiler overhead makes its Score unsuitable for the comparison.

## Share Evidence and Trade-offs

Include the following in the performance-fix PR:

- Baseline and Candidate commit SHAs;
- the exact benchmark method and workload parameters;
- JDK, JVM settings, and relevant machine information;
- the comparison report, raw JMH results, and variation across runs;
- correctness and compatibility checks for the changed path;
- regressions, resource trade-offs, and anything the experiment does not verify.
