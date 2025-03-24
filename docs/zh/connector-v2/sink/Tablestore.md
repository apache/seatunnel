import ChangeLog from '../changelog/connector-tablestore.md';

# Tablestore

> Tablestore 数据接收器

## 描述

用于将数据写入 Tablestore

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

|       名称        |  类型  | 是否必填 | 默认值 |
|-------------------|--------|----------|---------------|
| end_point         | string | 是      | -             |
| instance_name     | string | 是      | -             |
| access_key_id     | string | 是      | -             |
| access_key_secret | string | 是      | -             |
| table             | string | 是      | -             |
| primary_keys      | array  | 是      | -             |
| batch_size        | string | 否       | 25            |
| common-options    | config | 否       | -             |

### end_point [string]

endPoint 用于写入Tablestore。

### instanceName [string]

Tablestore 的实例名称。

### access_key_id [string]

Tablestore 访问的id。

### access_key_secret [string]

Tablestore 访问的密钥。

### table [string]

Tablestore的表。

### primaryKeys [array]

Tablestore 的主键。

### common 选项 [ config ]

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

## 示例

```bash
Tablestore {
    end_point = "xxxx"
    instance_name = "xxxx"
    access_key_id = "xxxx"
    access_key_secret = "xxxx"
    table = "sink"
    primary_keys = ["pk_1","pk_2","pk_3","pk_4"]
  }
```

## 变更日志

<ChangeLog />
