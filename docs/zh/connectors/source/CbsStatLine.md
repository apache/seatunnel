import ChangeLog from '../changelog/connector-http-cbs-statline.md';

# CBS StatLine

> CBS StatLine 源连接器

`CbsStatLine` 源连接器通过荷兰中央统计局（CBS）的 [OData v4 API](https://www.cbs.nl/nl-nl/onze-diensten/open-data/statline-als-open-data/snelstartgids-odata-v4) 读取一个公开 StatLine 数据集的 `Observations` 实体，无需凭据。每行对应一个观测单元格，不是透视后的数据集宽表行。

## 支持的特性

- 有界批量读取，源并行度为 `1`。
- 按服务端返回的 `@odata.nextLink` 翻页。不跟随重定向或离开当前数据集端点的翻页链接。
- 不支持增量读取、删除捕获、一致性时间点快照或精确一次回放。重跑从第一页开始。下游如需替换数据集，请在目标端选择覆盖或暂存后切换。
- 仅同步观测值。指标和维度的字典需要单独获取。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `dataset_id` | String | 是 | — | OData URL 中的 StatLine 数据集标识。 |
| `max_rows` | Integer | 否 | `0` | 正数限制输出行数，并为首次请求设置 OData `$top`；零表示读取全部可用页。 |
| `base_url` | String | 否 | `https://datasets.cbs.nl` | 提供相同 API 的源地址。测试网络之外请使用 HTTPS。 |

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `dataset_id` | STRING | 配置的数据集标识。 |
| `id` | BIGINT | 观测 ID，仅在当前数据集内使用。 |
| `measure` | STRING | CBS 指标代码。 |
| `value` | STRING | 数值以文本输出，避免双精度浮点舍入（可能使用指数表示法）；没有数值时为 null。转换目标类型前请确定所需精度。 |
| `string_value` | STRING | CBS 文本值（如有），保留尾部空格。 |
| `value_attribute` | STRING | CBS 数值属性（如有）。 |
| `observation` | STRING | API 观测对象的紧凑 JSON，保留当前数据集的所有维度。 |

## 示例

以下任务读取 CBS OData 入门指南所用公开数据集的两条记录。移除 `max_rows` 即可读取全部记录。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CbsStatLine {
    dataset_id = "83765NED"
    max_rows = 2
  }
}

sink {
  Console {}
}
```

HTTP 错误、无效响应或翻页循环会使任务失败，不会被当作读取完成。遇到临时不可用或限流时，可稍后重新运行批任务。

## 变更日志

<ChangeLog />
