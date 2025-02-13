# Tablestore

> Tablestore 水槽连接器

## 描述

将数据写入“Tablestore”`

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

|       名称        |  类型  | 需要 | 默认 值 |
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

endPoint用于写入Tablestore。

### instanceName [string]

Tablestore的实例名称。

### access_key_id [string]

Tablestore的访问id。

### access_key_secret [string]

Tablestore的访问秘密。

### table [string]

Tablestore的表。

### primaryKeys [array]

Tablestore的主键。

### common options [ config ]

Sink插件常用参数，请参考[Sink common Options]（../Sink common Options.md）了解详细信息。

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

## Changelog

### 下一个版本

- 添加Tablestore水槽连接器


