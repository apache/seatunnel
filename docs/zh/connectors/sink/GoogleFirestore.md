import ChangeLog from '../changelog/connector-google-firestore.md';

# GoogleFirestore

> Google Firestore Sink 连接器

## 描述

将数据写入 Google Firestore

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 选项

|    名称     |  类型  | 必需 | 默认值 |
|-------------|--------|------|--------|
| project_id  | string | 是   | -      |
| collection  | string | 是   | -      |
| credentials | string | 否   | -      |

### project_id [string]

Google Firestore 数据库项目的唯一标识符。

### collection [string]

Google Firestore 的集合。

### credentials [string]

Google Cloud 服务账户的凭证，使用 base64 编码。如果未设置，需要检查 `GOOGLE_APPLICATION_CREDENTIALS` 环境变量是否存在。

### 通用选项

汇插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详情。

## 示例

```bash
GoogleFirestore {
  project_id = "dummy-project-id",
  collection = "dummy-collection",
  credentials = "dummy-credentials"
}
```

## 变更日志

<ChangeLog />
