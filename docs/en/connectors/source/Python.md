import ChangeLog from '../changelog/connector-python.md';

# Python

> Python script source connector

## Description

Used to launch a Python script and read its stdout as SeaTunnel source records.

The Phase 1 MVP starts one Python process through `ProcessBuilder`, writes one JSON config object
to the first stdin line, then parses each stdout line with SeaTunnel text deserialization. This
keeps the Python side simple while still letting users reuse Python libraries for custom data
collection.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip Python contract

The current MVP supports `file_format_type = text` only.

- SeaTunnel writes the `python.script.config` object as JSON to the first stdin line.
- The Python script prints one record per stdout line.
- SeaTunnel parses each line with `field_delimiter` and converts fields according to `schema`.

:::

## Options

| name                     | type   | required | default value |
|--------------------------|--------|----------|---------------|
| python.executable        | string | no       | python3       |
| python.script.path       | string | yes      | -             |
| schema                   | config | yes      | -             |
| python.script.config     | map    | no       | {}            |
| python.working.directory | string | no       | script parent |
| file_format_type         | string | no       | text          |
| field_delimiter          | string | no       | ,             |
| common-options           |        | no       | -             |

### python.executable [string]

Python interpreter or executable used to start the script. The resolved absolute executable must be
listed in the operator-controlled system property
`seatunnel.source.python.allowed-executables`.

Examples: `python3`, `/usr/bin/python3`, `/opt/venv/bin/python`

### python.script.path [string]

Path of the Python script executed by the source connector.

### schema [config]

The schema fields of stdout records. SeaTunnel converts each delimited stdout line to a
`SeaTunnelRow` according to this schema. For more details, please refer to
[Schema Feature](../../introduction/concepts/schema-feature.md).

### python.script.config [map]

Optional config map serialized as JSON and written to the first stdin line of the Python process.

This is useful for passing API endpoints, tokens, filters, or other runtime arguments without
hardcoding them into the script.

### python.working.directory [string]

Optional working directory for the Python process. If unset, SeaTunnel uses the parent directory of
`python.script.path`.

### file_format_type [string]

Stdout parsing format. Phase 1 supports only:

- `text`

### field_delimiter [string]

Field delimiter used when `file_format_type = text`.

Examples: `,`, `|`, `\t`

### common options

Source plugin common parameters, please refer to
[Source Common Options](../common-options/source-common-options.md) for details.

## Security

- This connector is disabled by default. Operators must set
  `-Dseatunnel.source.python.enabled=true` and configure
  `-Dseatunnel.source.python.allowed-executables=/absolute/path/to/python3` on every worker node.
  Job configuration cannot enable the connector or widen this allowlist.
- `python.executable` and `python.script.path` run on the worker host with the privileges of the
  SeaTunnel worker process.
- Every process start logs the resolved executable and normalized `python.script.path` as an audit
  warning.
- `python.script.config` is serialized to JSON and written to the child process stdin, so secrets
  placed there are exposed to that child process and its runtime logs or diagnostics.
- Cluster operators should restrict who can submit jobs that use this connector and run workers in
  an appropriately sandboxed environment when possible.

## Example

### SeaTunnel config

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Python {
    plugin_output = "python_source"
    python.executable = "/usr/bin/python3"
    python.script.path = "/tmp/python_source.py"
    python.script.config = {
      prefix = "seatunnel"
      count = 3
    }
    file_format_type = "text"
    field_delimiter = ","
    schema = {
      fields {
        id = int
        name = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "python_source"
  }
}
```

### Python script

```python
#!/usr/bin/env python3
import json
import sys


def main():
    config_line = sys.stdin.readline().strip()
    config = json.loads(config_line) if config_line else {}
    prefix = config.get("prefix", "python")
    count = int(config.get("count", 2))

    for index in range(1, count + 1):
        print(f"{index},{prefix}_{index}", flush=True)


if __name__ == "__main__":
    main()
```

## Limitations

- Phase 1 supports source only.
- Phase 1 supports `text` output only.
- The source is single-reader today, so source parallelism must remain `1`.
- The source keeps no resumable offset or checkpoint state. On failure recovery or restart, the
  Python script re-executes from the beginning and rows already delivered downstream are emitted
  again; use idempotent sinks or make sure the job tolerates duplicates.
- The connector manages only the direct process. Scripts must not detach long-lived child processes
  that inherit stdout or stderr; use worker-level sandboxing and supervision for subprocess trees.
- Non-zero Python exits fail the source task and include recent stderr lines in the exception.

## Changelog

<ChangeLog />
