# SeaTunnel With Flink

Apache Flink is a strong choice when your team already operates Flink clusters and wants SeaTunnel jobs to reuse that runtime platform. If you are evaluating SeaTunnel for the first time and do not have existing Flink operations, start with [SeaTunnel Engine](./zeta/about.md) first and come back here only when Flink is the real requirement.

## Start Here

Use this path if you want to run SeaTunnel on Flink:

- [Engine Overview](./overview.md)
- [Quick Start With Flink](../getting-started/locally/quick-start-flink.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)

## When To Choose Flink

Flink is usually the right engine when:

- your organization already runs Flink clusters in production
- you want to reuse existing Flink deployment, monitoring, and operational practices
- your jobs need to align with a broader Flink-based stream processing environment

## Flink-Specific Configuration

SeaTunnel job-level Flink configuration uses the `flink.` prefix inside the `env` block.

Example:

```hocon
env {
  parallelism = 1
  flink.execution.checkpointing.unaligned.enabled = true
}
```

Inline enumeration types are not fully supported in the SeaTunnel job config. For settings that require enum-like values outside the supported inline types, configure them in Flink itself. The common inline-supported value types are:

- `Integer`
- `Boolean`
- `String`
- `Duration`

## Flink 1.20.4 With Java 17: Runtime Preflight

The following conditions were verified with Flink 1.20.4 and Java 17. Check the actual Flink installation used by this job; configuration from another Flink version or cluster is not a substitute.

1. Record the submission client's `JAVA_HOME`, `PATH` and JVM options, and the JobManager and TaskManager launch configuration. Check the running cluster processes as well as the configuration files: changing a file does not change an already running JVM. The SeaTunnel launcher sources `$SEATUNNEL_HOME/config/seatunnel-env.sh`, so verify the effective `FLINK_HOME` and Java settings after that file is loaded.
2. Add `--add-opens=java.base/java.util=ALL-UNNAMED` to the Java 17 submission client and TaskManager JVMs, preserving their existing options. Without it, the client can fail while registering the process-function translator, or the TaskManager can fail in the Kryo/Chill serializer with `InaccessibleObjectException`. Apply the TaskManager option through the process manager or launch environment that actually starts this installation, then restart the affected process. For an isolated manual launch, append the option to `JAVA_TOOL_OPTIONS` in both the client and cluster launch environments:

   ```bash
   export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }--add-opens=java.base/java.util=ALL-UNNAMED"
   ```

   A client-only setting does not update an existing TaskManager. The verified standalone setup used this launch environment for both JobManager and TaskManager; the observed reflection failures required the option on the client and TaskManager.
3. If lineage is enabled, install exactly one matching `seatunnel-lineage-flink-<version>-shaded.jar` in the JobManager's `$FLINK_HOME/lib` and restart the JobManager. Check that it contains `org/apache/seatunnel/lineage/flink/LineageJobStatusHook.class`. Sending the hook only inside the submitted job jar is insufficient. See [lineage artifact installation](../introduction/concepts/lineage.md#installing-the-lineage-artifact-on-the-cluster) for configuration and token handling.
4. Query the target cluster's REST API before submission:

   ```bash
   curl --fail --silent --show-error "$FLINK_REST_URL/overview"
   curl --fail --silent --show-error "$FLINK_REST_URL/taskmanagers"
   ```

   Require `flink-version` to match the intended runtime, at least one registered TaskManager and enough `slots-available` for the configured job parallelism. A reachable JobManager with zero TaskManagers or slots is not ready to execute the job.

### Reproduce And Restore

Use a bounded job with a known source dataset and a sink whose complete contents can be read back. Install its connectors first, then submit through the Flink 1.20 launcher:

```bash
# Attached
bash "$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh" \
  --config "$JOB_CONFIG" --target remote -n "$JOB_NAME"
# Detached: use a separate sink and job name for this run
bash "$SEATUNNEL_HOME/bin/start-seatunnel-flink-20-connector-v2.sh" \
  --config "$DETACHED_JOB_CONFIG" --target remote -n "$DETACHED_JOB_NAME" -d
```

For each submission, capture the JobID, wait for `FINISHED`, compare all ordered sink rows with the source, and verify one `START` and one final `COMPLETE` for each expected lineage run. Detached submission returning successfully only confirms submission; inspect the JobManager result and receiver events. Re-read events after completion to check that no duplicate terminal event or late heartbeat appeared.

On an isolated test installation, also verify that removing the shaded hook jar and restarting the JobManager makes an enabled job fail explicitly at submission, while `openlineage_enabled=false` still allows the data job to finish without lineage events.

Before changing the installation, save its configuration, lib jar list, launch environment and running-role/slot state. Record the exact changes and restart commands. After testing, restore those settings and jar files, restart only the roles that were originally running, and compare the restored state with the saved baseline. An original baseline with zero TaskManagers and zero slots is a valid restoration target, even though it is unsuitable for a test submission.

## Minimal Example Job

The example below runs on Flink and prints generated records to the console.

```hocon
env {
  parallelism = 1
  checkpoint.interval = 5000

  flink.execution.checkpointing.mode = "EXACTLY_ONCE"
  flink.execution.checkpointing.timeout = 600000
}

source {
  FakeSource {
    row.num = 16
    plugin_output = "fake_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(33, 18)"
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake_table"
    plugin_output = "fake_output"
    field_mapper = {
      c_string = c_string
      c_int = c_int
    }
  }
}

sink {
  Console {
    plugin_input = "fake_output"
  }
}
```

If you need more transform options, see [Transforms Catalog](../transforms) and [Transform Common Options](../transforms/common-options/common-options.md).

## Running From A Source Checkout

If you are running examples from the repository source tree, the example module is:

- `seatunnel-examples/seatunnel-flink-connector-v2-example`

The example entry point is:

- `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample`

## Next Steps

- [Quick Start With Flink](../getting-started/locally/quick-start-flink.md)
- [Flink Translation Layer](../architecture/api-design/flink-translation-layer.md)
- [Transforms Catalog](../transforms)
- [SeaTunnel Engine](./zeta/about.md) if you want to compare against the default engine
