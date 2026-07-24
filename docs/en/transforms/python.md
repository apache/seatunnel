# Python

> Python transform plugin

## Description

The Python transform lets you run custom Python logic for each input row and append the returned fields to the downstream schema.

SeaTunnel keeps one long-lived Python worker process for each transform instance. The worker receives rows as JSON, runs your `process(row, context)` function, and converts the result back into the configured SeaTunnel field types.

## Options

| name | type | required | default value |
|------|------|----------|---------------|
| source_code | string | no | |
| source_code_path | string | no | |
| python_executable | string | no | python3 |
| script_config | map | no | |
| columns | array | yes | |
| row_error_handle_way | enum | no | FAIL |

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options/common-options.md) for details.

### source_code [string]

Inline Python source code. Exactly one of `source_code` or `source_code_path` must be configured.

### source_code_path [string]

Absolute or runtime-visible path of the Python script on the SeaTunnel worker host. Exactly one of `source_code` or `source_code_path` must be configured.

### python_executable [string]

Python executable used to start the worker process. The default value is `python3`. When the default value is used, SeaTunnel will also try `python` as a fallback after resolving both commands from `PATH`.

When the transform is enabled, the executable actually launched must be present in the server-side system property `seatunnel.transform.python.allowed-executables`. In production, prefer setting `python_executable` to an absolute path such as `/usr/bin/python3`.

### script_config [map]

Optional static user configuration injected into the Python runtime context as `context["config"]`.

### row_error_handle_way [enum]

Controls what happens when the Python script fails for a row.

- `FAIL`: stop the job and surface the Python error.
- `SKIP`: skip the current row and continue processing later rows.

### columns [array]

Declares the fields appended by the Python transform.

#### option

| name | type | required | default value |
|------|------|----------|---------------|
| dest_field | string | yes | |
| dest_type | string | no | string |

#### dest_field [string]

Output field name returned by the Python script.

#### dest_type [string]

SeaTunnel type used to convert the script result for `dest_field`. If omitted, the type defaults to `string`.

## Python Script Contract

Your script must define:

- `process(row, context)`

It can also define:

- `open(context)`
- `close()`

### row

`row` is a JSON-style object keyed by the input field names.

### context

`context` contains:

- `input_fields`: ordered input schema metadata
- `output_fields`: ordered output schema metadata
- `config`: the `script_config` map from the job config

### Return value

The `process` function can return one of these shapes:

- an object keyed by `dest_field`
- an array aligned with the `columns` order
- a scalar value when only one output column is configured

If the return shape does not match the declared `columns`, SeaTunnel will fail the row. Object results must contain every declared `dest_field`; an explicitly returned `null` is accepted, but a missing field is not.

## Security

- This transform is disabled by default. Operators must set `-Dseatunnel.transform.python.enabled=true` and configure `-Dseatunnel.transform.python.allowed-executables=/absolute/path/to/python3,/absolute/path/to/python` on every worker node before any job can start a Python worker.
- This transform runs the user-configured `python_executable` and Python code without a sandbox, using the operating-system permissions of the SeaTunnel worker process. Because `python_executable` can reference any executable, operators must restrict who can submit or modify jobs that use this transform.
- SeaTunnel logs the resolved interpreter path and script origin every time a Python worker starts so operators can audit usage.
- Only run trusted scripts, and do not place secrets in `source_code` or `script_config`.
- SeaTunnel manages only the direct Python worker process. Child processes started by user code are not terminated as a process tree and must be bounded by an external sandbox or process supervisor.

## Notes

- The runtime host must have Python installed.
- `source_code_path` must exist on every runtime node that executes the transform.
- Regular `print(...)` output from the user script is redirected to stderr so it does not break the row protocol.
- Writing directly to stdout through `sys.stdout`, native libraries, or child processes is not supported because stdout is reserved for the worker protocol.
- A failure in the optional `close()` hook is reported by transform cleanup and logged by the runtime. Transform cleanup is best-effort, so this failure does not change an already completed job's terminal state. Keep cleanup bounded and monitor worker logs for cleanup failures.
- Avoid long-running blocking logic in `process(...)` because every row waits for the Python worker response.

## Example: Inline Script

```hocon
transform {
  Python {
    plugin_input = "fake"
    plugin_output = "python_out"
    python_executable = "/usr/bin/python3"
    script_config = {
      prefix = "user:"
    }
    columns = [
      {
        dest_field = normalized_name
        dest_type = string
      },
      {
        dest_field = age_plus_one
        dest_type = int
      }
    ]
    source_code = """
def process(row, context):
    return {
        "normalized_name": context["config"]["prefix"] + row["name"].strip().lower(),
        "age_plus_one": row["age"] + 1,
    }
"""
  }
}
```

## Example: Runtime Script Path

```hocon
transform {
  Python {
    plugin_input = "fake"
    plugin_output = "python_out"
    python_executable = "/usr/bin/python3"
    source_code_path = "/tmp/python_transform.py"
    columns = [
      {
        dest_field = normalized_name
        dest_type = string
      },
      {
        dest_field = age_plus_one
        dest_type = int
      }
    ]
  }
}
```
