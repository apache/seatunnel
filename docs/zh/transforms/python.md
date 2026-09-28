# Python

> Python 转换插件

## 描述

Python Transform 允许你为每一行输入数据执行自定义 Python 逻辑，并把脚本返回的字段追加到下游 schema 中。

SeaTunnel 会为每个 Transform 实例维护一个长生命周期的 Python Worker 进程。Worker 通过 JSON 接收行数据，执行你的 `process(row, context)` 函数，再把结果按声明的 SeaTunnel 类型转换回来。

## 属性

| 名称 | 类型 | 是否必须 | 默认值 |
|------|------|----------|--------|
| source_code | string | 否 | |
| source_code_path | string | 否 | |
| python_executable | string | 否 | python3 |
| script_config | map | 否 | |
| columns | array | 是 | |
| row_error_handle_way | enum | 否 | FAIL |

### 通用选项 [string]

转换插件的常见参数，请参考 [Transform Plugin](common-options/common-options.md) 了解详情。

### source_code [string]

内联 Python 源码。`source_code` 与 `source_code_path` 必须且只能配置其中一个。

### source_code_path [string]

SeaTunnel Worker 运行节点上可见的 Python 脚本路径。`source_code` 与 `source_code_path` 必须且只能配置其中一个。

### python_executable [string]

启动 Worker 进程时使用的 Python 可执行文件，默认值为 `python3`。当使用默认值时，SeaTunnel 会先从 `PATH` 解析 `python3`，失败后再解析 `python` 作为回退。

当该 Transform 被启用后，最终实际启动的解释器必须出现在服务端系统属性 `seatunnel.transform.python.allowed-executables` 中。生产环境建议把 `python_executable` 显式设置为绝对路径，例如 `/usr/bin/python3`。

### script_config [map]

可选的静态用户配置，会注入到 Python 运行上下文中的 `context["config"]`。

### row_error_handle_way [enum]

控制某一行执行 Python 脚本失败时的处理方式。

- `FAIL`：终止任务，并抛出 Python 错误。
- `SKIP`：跳过当前行，继续处理后续数据。

### columns [array]

声明 Python Transform 追加的输出字段。

#### 子属性

| 名称 | 类型 | 是否必须 | 默认值 |
|------|------|----------|--------|
| dest_field | string | 是 | |
| dest_type | string | 否 | string |

#### dest_field [string]

Python 脚本返回的输出字段名。

#### dest_type [string]

`dest_field` 对应的 SeaTunnel 类型。如果省略，默认使用 `string`。

## Python 脚本约定

脚本必须定义：

- `process(row, context)`

脚本也可以定义：

- `open(context)`
- `close()`

### row

`row` 是一个以输入字段名为 key 的 JSON 风格对象。

### context

`context` 中包含：

- `input_fields`：有序的输入 schema 元数据
- `output_fields`：有序的输出 schema 元数据
- `config`：任务配置中的 `script_config` 映射

### 返回值

`process` 函数可以返回以下三种结构之一：

- 以 `dest_field` 为 key 的对象
- 与 `columns` 顺序一致的数组
- 当只声明了一个输出列时，直接返回单个标量值

如果返回结构和声明的 `columns` 不匹配，SeaTunnel 会把这行数据视为失败。对象结果必须包含每一个已声明的 `dest_field`；显式返回 `null` 是允许的，但缺少字段会失败。

## 安全

- 该 Transform 默认禁用。集群管理员必须在每个 Worker 节点上同时设置 `-Dseatunnel.transform.python.enabled=true` 和 `-Dseatunnel.transform.python.allowed-executables=/absolute/path/to/python3,/absolute/path/to/python`，任务才能启动 Python Worker。
- 本 Transform 会在没有沙箱隔离的情况下运行用户配置的 `python_executable` 和 Python 代码，并继承 SeaTunnel Worker 进程的操作系统权限。由于 `python_executable` 可以指向任意可执行文件，集群管理员必须限制可提交或修改此类任务的用户范围。
- 每次启动 Python Worker 时，SeaTunnel 都会把最终解析到的解释器绝对路径和脚本来源写入安全告警日志，便于审计。
- 请只运行可信脚本，不要在 `source_code` 或 `script_config` 中放置密钥等敏感信息。
- SeaTunnel 只管理直接启动的 Python Worker 进程，不会按进程树终止用户代码创建的子进程；这类进程必须由外部沙箱或进程监管器限制。

## 注意事项

- 运行节点必须安装 Python。
- `source_code_path` 指向的文件必须存在于每个实际执行该 Transform 的运行节点上。
- 用户脚本中的普通 `print(...)` 会被重定向到 stderr，避免破坏 Worker 的 stdout 通讯协议。
- 不支持通过 `sys.stdout`、原生库或子进程直接写 stdout，因为 stdout 专用于 Worker 通讯协议。
- 可选的 `close()` hook 执行失败会由 Transform 清理流程报告并记录到运行时日志。Transform 清理采用 best-effort 语义，因此该错误不会改变已经完成的 Job 终态。清理逻辑应保持有界，并监控 Worker 日志中的清理失败。
- 如果 `process(...)` 里执行了耗时阻塞逻辑，每一行都会等待 Python Worker 返回结果，可能明显影响吞吐。

## 示例：内联脚本

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

## 示例：运行时脚本路径

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
