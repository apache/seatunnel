# DataValidator

> 数据验证转换插件

## 描述

DataValidator 转换插件根据配置的规则验证字段值，并基于指定的错误处理策略处理验证失败的情况。它支持多种验证规则类型，包括空值检查、范围验证、长度验证和正则表达式模式匹配。

## 选项

|      名称       |  类型  | 是否必需 | 默认值 |
|-----------------|--------|----------|--------|
| error_handle_way| enum   | 否       | FAIL   |
| error_table     | string | 否       |        |
| field_rules     | array  | 是       |        |

### error_handle_way [enum]

验证失败时的错误处理策略：
- `FAIL`: 当验证错误发生时，整个任务失败
- `SKIP`: 跳过无效行并继续处理
- `ROUTE_TO_TABLE`: 将无效数据路由到指定的错误表

**注意**: `ROUTE_TO_TABLE` 模式仅适用于支持多表的 sink 连接器。sink 必须具备处理路由到不同表目标的数据的能力。

### error_table [string]

当 `error_handle_way` 设置为 `ROUTE_TO_TABLE` 时，用于路由无效数据的目标表名。使用 `ROUTE_TO_TABLE` 模式时此参数为必需。

### field_rules [array]

字段验证规则数组。每个规则定义特定字段的验证条件。

#### 字段规则结构

每个字段规则包含：
- `field_name`: 要验证的字段名称
- `rules`: 要应用的验证规则数组（嵌套格式），或单独的规则属性（扁平格式）

#### 验证规则类型

##### NOT_NULL
验证字段值不为空。

参数：
- `rule_type`: "NOT_NULL"
- `custom_message` (可选): 自定义错误消息

##### RANGE
验证数值在指定范围内。

参数：
- `rule_type`: "RANGE"
- `min_value` (可选): 最小允许值
- `max_value` (可选): 最大允许值
- `min_inclusive` (可选): 最小值是否包含在内（默认: true）
- `max_inclusive` (可选): 最大值是否包含在内（默认: true）
- `custom_message` (可选): 自定义错误消息

##### LENGTH
验证字符串、数组或集合值的长度。

参数：
- `rule_type`: "LENGTH"
- `min_length` (可选): 最小允许长度
- `max_length` (可选): 最大允许长度
- `exact_length` (可选): 精确要求的长度
- `custom_message` (可选): 自定义错误消息

##### REGEX
验证字符串值匹配正则表达式模式。

参数：
- `rule_type`: "REGEX"
- `pattern`: 正则表达式模式（必需）
- `case_sensitive` (可选): 模式匹配是否区分大小写（默认: true）
- `custom_message` (可选): 自定义错误消息

### 通用选项 [string]

转换插件通用参数，请参考 [Transform Plugin](common-options.md) 了解详情

## 示例

### 示例 1: 使用 FAIL 模式的基本验证

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    error_handle_way = "FAIL"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = 0
        max_value = 150
      },
      {
        field_name = "email"
        rule_type = "REGEX"
        pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
      }
    ]
  }
}
```

### 示例 2: 使用 SKIP 模式的验证

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    error_handle_way = "SKIP"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "name"
        rule_type = "LENGTH"
        min_length = 2
        max_length = 50
      }
    ]
  }
}
```

### 示例 3: 使用 ROUTE_TO_TABLE 模式的验证

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    error_handle_way = "ROUTE_TO_TABLE"
    error_table = "error_data"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = 0
        max_value = 150
      }
    ]
  }
}
```

**注意**: 使用 `ROUTE_TO_TABLE` 时，请确保您的 sink 连接器支持多表。有效数据将发送到主输出表，而无效数据将路由到指定的错误表。

### 示例 4: 嵌套规则格式

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    error_handle_way = "FAIL"
    field_rules = [
      {
        field_name = "name"
        rules = [
          {
            rule_type = "NOT_NULL"
            custom_message = "姓名是必需的"
          },
          {
            rule_type = "LENGTH"
            min_length = 2
            max_length = 50
            custom_message = "姓名长度必须在2到50个字符之间"
          }
        ]
      }
    ]
  }
}
```

## 更新日志

### 新版本
- 添加 DataValidator 转换连接器
- 支持 NOT_NULL、RANGE、LENGTH 和 REGEX 验证规则
- 支持 FAIL、SKIP 和 ROUTE_TO_TABLE 错误处理模式
- 支持扁平和嵌套规则配置格式
