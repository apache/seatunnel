import ChangeLog from '../changelog/connector-python.md';

# PythonSource

> Python 脚本源连接器

## 描述

用于拉起一个 Python 脚本，并把它的 stdout 读取为 SeaTunnel Source 数据。

当前 Phase 1 MVP 通过 `ProcessBuilder` 启动一个 Python 进程，把 `python.script.config`
序列化成 JSON 写到 stdin 第一行，再把 stdout 的每一行按 text 格式解析成 SeaTunnel
Row。这样 Python 侧只需要专注于取数和打印文本行，实现门槛比较低。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义 split](../../introduction/concepts/connector-v2-features.md)

:::tip Python 交互约定

当前 MVP 只支持 `file_format_type = text`。

- SeaTunnel 会把 `python.script.config` 作为 JSON 写到 stdin 第一行。
- Python 脚本每打印一行 stdout，就表示一条记录。
- SeaTunnel 会按 `field_delimiter` 切分该行，并依据 `schema` 做类型转换。

:::

## 选项

| 参数名                    | 类型     | 必须 | 默认值        |
|------------------------|--------|----|------------|
| python.executable      | string | 否  | python3    |
| python.script.path     | string | 是  | -          |
| schema                 | config | 是  | -          |
| python.script.config   | map    | 否  | {}         |
| python.working_directory | string | 否 | 脚本所在目录 |
| file_format_type       | string | 否  | text       |
| field_delimiter        | string | 否  | ,          |
| common-options         |        | 否  | -          |

### python.executable [string]

用于启动脚本的 Python 解释器或可执行文件。

示例：`python3`、`/usr/bin/python3`、`/opt/venv/bin/python`

### python.script.path [string]

要执行的 Python 脚本路径。

### schema [config]

stdout 记录的 schema。SeaTunnel 会按这个 schema 把每一行 text 输出转换成
`SeaTunnelRow`。更多详情请参考
[Schema 特性](../../introduction/concepts/schema-feature.md)。

### python.script.config [map]

可选配置对象。SeaTunnel 会把它序列化成 JSON，写入 Python 进程 stdin 的第一行。

适合传递 API 地址、鉴权信息、过滤条件或其他运行时参数，避免把这些配置硬编码进脚本。

### python.working_directory [string]

Python 进程的工作目录。未配置时，默认使用 `python.script.path` 的父目录。

### file_format_type [string]

stdout 解析格式。当前 Phase 1 只支持：

- `text`

### field_delimiter [string]

当 `file_format_type = text` 时使用的字段分隔符。

示例：`,`, `|`, `\t`

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

## 示例

### SeaTunnel 配置

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  PythonSource {
    python.executable = "python3"
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
    plugin_input = "PythonSource"
  }
}
```

### Python 脚本

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

## 限制

- Phase 1 只支持 source。
- Phase 1 只支持 `text` 输出格式。
- 当前 Source 只有单 reader，因此 source parallelism 必须保持为 `1`。
- 如果 Python 进程非零退出，Source task 会失败，并在异常里带上最近的 stderr 输出。

## 变更日志

<ChangeLog />
