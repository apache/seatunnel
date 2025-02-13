# GoogleFirestore

> 谷歌Firestore水槽连接器

## 描述

将数据写入Google Firestore

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

|    名称     |  类型  | 需要 | 默认 值 |
|-------------|--------|----------|---------------|
| project_id  | string | 是      | -             |
| collection  | string | 是      | -             |
| credentials | string | 否       | -             |

### project_id [string]

Google Firestore数据库项目的唯一标识符。

### collection [string]

Google Firestore的合集。

### credentials [string]

Google Cloud服务帐户的凭据，使用base64编解码器。如果没有设置，需要检查是否存在“GOOGLE APPLICATION CREDENTIALS”环境。

### common options

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

## 示例

```bash
GoogleFirestore {
  project_id = "dummy-project-id",
  collection = "dummy-collection",
  credentials = "dummy-credentials"
}  
```

## Changelog

### 下一个版本

- 添加Google Firestore接收器连接器

