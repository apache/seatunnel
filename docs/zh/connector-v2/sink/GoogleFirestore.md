# GoogleFirestore

> GoogleFirestore 数据接收器

## 描述

用于将数据写入 GoogleFirestore

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

|    名称     |  类型  | 是否必传 | 默认值 |
|-------------|--------|----------|---------------|
| project_id  | string | 是      | -             |
| collection  | string | 是      | -             |
| credentials | string | 否       | -             |

### project_id [string]

GoogleFirestore 数据库项目的唯一标识符。

### collection [string]

GoogleFirestore 的合集。

### credentials [string]

GoogleCloud 服务帐户的凭据，使用base64编解码器。如果没有设置，需要检查 `GOOGLE APPLICATION CREDENTIALS` 环境是否存在。

### common 选项

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

## 示例

```bash
GoogleFirestore {
  project_id = "dummy-project-id",
  collection = "dummy-collection",
  credentials = "dummy-credentials"
}  
```

## 更改日志

### 随后版本

- 添加 GoogleFirestore 数据接收器

