# 飞书

> 飞书 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [变更数据捕获](../../concept/connector-v2-features.md)

## 描述

用于通过数据调用飞书的web hooks。

> 例如，如果来自上游的数据是 [`年龄: 12, 姓名: tyrantlucifer`]，则 body 内容如下：`{"年龄": 12, "姓名": "tyrantlucifer"}`

**提示：飞书接收器仅支持 `post json`类型的web hook，并且源数据将被视为web hook的正文内容。**

## 数据类型映射

|       SeaTunnel 数据类型        |   飞书数据类型   |
|-----------------------------|------------|
| ROW<br/>MAP                 | Json       |
| NULL                        | null       |
| BOOLEAN                     | boolean    |
| TINYINT                     | byte       |
| SMALLINT                    | short      |
| INT                         | int        |
| BIGINT                      | long       |
| FLOAT                       | float      |
| DOUBLE                      | double     |
| DECIMAL                     | BigDecimal |
| BYTES                       | byte[]     |
| STRING                      | String     |
| TIME<br/>TIMESTAMP<br/>TIME | String     |
| ARRAY                       | JsonArray  |

## 接收器选项

|       名称       |   类型   | 是否必需 | 默认值 |                             描述                             |
|----------------|--------|------|-----|------------------------------------------------------------|
| url            | String | 是    | -   | 飞书web hook URL                                             |
| headers        | Map    | 否    | -   | HTTP 请求头                                                   |
| common-options |        | 否    | -   | 接收器插件常见参数，请参阅 [接收器通用选项](../sink-common-options.md) 以获取详细信息 |

## 任务示例

### 简单示例:

```hocon
Feishu {
        url = "https://www.feishu.cn/flow/api/trigger-webhook/108bb8f208d9b2378c8c7aedad715c19"
    }
```

## 更新日志

### 2.2.0-beta 2022-09-26

- 添加飞书接收器

