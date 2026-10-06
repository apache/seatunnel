import ChangeLog from '../changelog/connector-python.md';

# Python

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
| python.working.directory | string | 否 | 脚本所在目录 |
| file_format_type       | string | 否  | text       |
| field_delimiter        | string | 否  | ,          |
| common-options         |        | 否  | -          |

### python.executable [string]

用于启动脚本的 Python 解释器或可执行文件。最终解析出的绝对路径必须包含在集群管理员控制的系统属性
`seatunnel.source.python.allowed-executables` 中。

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

### python.working.directory [string]

Python 进程的工作目录。未配置时，默认使用 `python.script.path` 的父目录。

### file_format_type [string]

stdout 解析格式。当前 Phase 1 只支持：

- `text`

### field_delimiter [string]

当 `file_format_type = text` 时使用的字段分隔符。

示例：`,`, `|`, `\t`

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。

## 安全说明

- 该连接器默认禁用。集群管理员必须在每个 Worker 节点设置
  `-Dseatunnel.source.python.enabled=true`，并配置
  `-Dseatunnel.source.python.allowed-executables=/absolute/path/to/python3`。任务配置不能启用该连接器，也不能扩大该白名单。
- `python.executable` 和 `python.script.path` 会在 worker 节点上以 SeaTunnel worker 进程的权限直接执行。
- 每次启动进程时，SeaTunnel 都会把最终解析出的可执行文件和规范化后的 `python.script.path` 作为审计告警写入日志。
- `python.script.config` 会被序列化为 JSON 并写入子进程 stdin，因此其中的密钥或令牌会暴露给该子进程及其运行期日志或诊断信息。
- 在共享集群中，建议限制谁可以提交使用该连接器的任务，并尽量让 worker 运行在受控或隔离的环境里。

## 示例

### SeaTunnel 配置

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
- Source 不保存任何可恢复的位点或 checkpoint 状态。任务失败恢复或重启后，Python 脚本会从头
  重新执行，之前已经下发的行会再次发出；请使用幂等的 sink，或确保任务可以容忍重复数据。
- 连接器只管理直接启动的进程。脚本不能派生继承 stdout 或 stderr 的长期后台子进程；如需
  管理子进程树，应由 worker 侧的隔离与进程监管机制负责。
- 如果 Python 进程非零退出，Source task 会失败，并在异常里带上最近的 stderr 输出。

## 变更日志

<ChangeLog />
