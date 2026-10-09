# Http-Linear

> Http-Linear 源连接器

## 描述

通过 HTTP 从 Linear API 读取数据。

## 关键特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次

## 选项

| 名称           | 类型     | 必填 | 默认值 | 描述                     |
|----------------|----------|------|--------|--------------------------|
| url            | String   | 是   | -      | Linear API 接口地址      |
| api_key        | String   | 是   | -      | 用于身份验证的 Linear API Key |
| plugin_output  | String   | 否   | -      | 生成数据时使用的结果表名 |

## 示例

```hocon
source {
  Http-Linear {
    url = "https://api.linear.app/graphql"
    api_key = "your_linear_api_key"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    plugin_output = "linear_data"
  }
}
```
