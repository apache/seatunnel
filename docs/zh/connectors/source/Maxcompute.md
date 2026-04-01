import ChangeLog from '../changelog/connector-maxcompute.md';

# Maxcompute

> Maxcompute 源连接器

## 描述

用于从 Maxcompute 读取数据.

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           |  类型  | 必需 | 默认值 |
|----------------|--------|----|---------------|
| accessId       | string | 是  | -             |
| accesskey      | string | 是  | -             |
| endpoint       | string | 是  | -             |
| project        | string | 是  | -             |
| table_name     | string | 是  | -             |
| partition_spec | string | 否  | -             |
| split_row      | int    | 否 | 10000         |
| read_columns   | Array  | 否 | -             |
| table_list     | Array  | 否 | -             |
| tunnel_endpoint| string | 否 | -             |
| tunnel_name    | string | 否 | -             |
| common-options | string | 否 |               |
| schema         | config | 否 |               |

### accessId [string]

`accessId` 您的 Maxcompute 密钥 Id, 可以从阿里云访问哪个云.

### accesskey [string]

`accesskey` Your Maxcompute 密钥, 可以从阿里云访问哪个云.

### endpoint [string]

`endpoint` 您的 Maxcompute 端点以 http 开头.

### project [string]

`project` 您在阿里云中创建的Maxcompute项目.

### table_name [string]

`table_name` 目标Maxcompute表名，例如：fake.

### partition_spec [string]

`partition_spec` Maxcompute分区表的此规范，例如:ds='20220101'.

### split_row [int]

`split_row` 每次拆分的行数，默认值: 10000.

### read_columns [Array]

`read_columns` 要读取的列，如果未设置，则将读取所有列。例如. ["col1", "col2"]

### table_list [Array]

要读取的表列表，您可以使用此配置代替 `table_name`.

### tunnel_endpoint [String]

指定 MaxCompute Tunnel 服务的自定义端点 URL。

默认情况下，端点是从配置的区域自动推断的。

此选项允许您覆盖默认行为并使用自定义 Tunnel 端点。
如果未指定，连接器将使用基于区域的默认 Tunnel 端点。

通常，您**不需要**设置 tunnel_endpoint。仅在自定义网络、调试或本地开发时才需要。

示例值：

- `https://dt.cn-hangzhou.maxcompute.aliyun.com`
- `https://dt.ap-southeast-1.maxcompute.aliyun.com`
- `http://maxcompute:8080`

默认值：未设置（从区域自动推断）

### tunnel_name [String]

`tunnel_name` 指定 Tunnel Quota 名称，用于独占资源组。

Tunnel Quota 允许您使用专用的计算资源进行 MaxCompute Tunnel 数据传输，从而提供更好的性能和资源隔离。

**重要提示**：Tunnel Quota 仅在 **VPC（虚拟私有云）端点**下生效，暂不支持公共网络访问。使用 `tunnel_name` 时，必须同时配置 `endpoint` 和 `tunnel_endpoint` 为 VPC 端点。

如果未指定，将使用默认的 Tunnel quota。

示例值：

- `your_tunnel_quota_name`

默认值：未设置（使用默认 quota）

### common options

源插件常用参数, 详见 [源通用选项](../common-options/source-common-options.md) .

## 示例

### 表读取

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    table_name="<your table name>"
    #partition_spec="<your partition spec>"
    #split_row = 10000
    #read_columns = ["col1", "col2"]
  }
}
```

### 使用表列表读取

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>" # default project
    table_list = [
      {
        table_name = "test_table"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      },
      {
        project = "test_project"
        table_name = "test_table2"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      }
    ]
  }
}
```

## 变更日志

<ChangeLog />
