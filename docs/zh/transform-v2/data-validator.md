# DataValidator

> 数据验证转换插件

## 描述

DataValidator 转换插件根据配置的规则验证字段值，并基于指定的错误处理策略处理验证失败的情况。它支持多种验证规则类型，包括空值检查、范围验证、长度验证和正则表达式模式匹配。

## 选项

|      名称       |  类型  | 是否必需 | 默认值 |
|-----------------|--------|----------|--------|
| error_handle_way| enum   | 否       | FAIL   |
| row_error_handle_way.error_table     | string | 否       |        |
| field_rules     | array  | 是       |        |

### row_error_handle_way [enum]

验证失败时的错误处理策略：
- `FAIL`: 当验证错误发生时，整个任务失败
- `SKIP`: 跳过无效行并继续处理
- `ROUTE_TO_TABLE`: 将无效数据路由到指定的错误表

**注意**: `ROUTE_TO_TABLE` 模式仅适用于支持多表的 sink 连接器。sink 必须具备处理路由到不同表目标的数据的能力。

### row_error_handle_way.error_table [string]

当 `row_error_handle_way` 设置为 `ROUTE_TO_TABLE` 时，用于路由无效数据的目标表名。使用 `ROUTE_TO_TABLE` 模式时此参数为必需。

#### 错误表Schema

当使用 `ROUTE_TO_TABLE` 模式时，DataValidator会自动创建一个具有固定schema的错误表来存储验证失败的数据。错误表包含以下字段：

| 字段名 | 数据类型 | 描述 |
|--------|----------|------|
| source_table_id | STRING | 源表标识符，标识数据来源的表 |
| source_table_path | STRING | 源表路径，完整的表路径信息 |
| original_data | STRING | 原始数据的JSON表示，包含验证失败的完整行数据 |
| validation_errors | STRING | 验证错误详情的JSON数组，包含所有验证失败的字段和错误信息 |
| create_time | TIMESTAMP | 验证错误的创建时间 |

**完整错误表记录示例**：
```json
{
  "source_table_id": "users_table",
  "source_table_path": "database.users",
  "original_data": "{\"id\": 123, \"name\": null, \"age\": 200, \"email\": \"invalid-email\"}",
  "validation_errors": "[{\"field_name\": \"name\", \"error_message\": \"Field 'name' cannot be null\"}, {\"field_name\": \"age\", \"error_message\": \"Field 'age' value 200 is not within range [0, 150]\"}, {\"field_name\": \"email\", \"error_message\": \"Field 'email' does not match pattern '^[\\\\w-\\\\.]+@([\\\\w-]+\\\\.)+[\\\\w-]{2,4}$'\"}]",
  "create_time": "2024-01-15T10:30:45"
}
```

**数据路由机制**：
- 验证通过的数据会保持原始schema并路由到主输出表
- 验证失败的数据会被转换为上述错误表schema格式并路由到指定的错误表
- 每个验证失败的行都会在错误表中生成一条记录，包含完整的原始数据和详细的错误信息

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

##### UDF (用户自定义函数)
使用自定义业务逻辑实现的用户自定义函数验证字段值。

参数：
- `rule_type`: "UDF"
- `function_name`: 要执行的UDF函数名称（必需）
- `custom_message` (可选): 自定义错误消息

**内置UDF函数：**
- `EMAIL`: 基于OWASP建议使用实用验证规则验证电子邮件地址

**创建自定义UDF函数：**
要创建自定义UDF函数：
1. 实现 `DataValidatorUDF` 接口
2. 使用 `@AutoService(DataValidatorUDF.class)` 注解
3. 提供唯一的 `functionName()`
4. 实现包含自定义逻辑的 `validate()` 方法

### 通用选项 [string]

转换插件通用参数，请参考 [Transform Plugin](common-options.md) 了解详情

## 示例

### 示例 1: 使用 FAIL 模式的基本验证

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    row_error_handle_way = "FAIL"
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
    row_error_handle_way = "SKIP"
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
    row_error_handle_way = "ROUTE_TO_TABLE"
    row_error_handle_way.error_table = "error_data"
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

在此示例中：
- 验证通过的数据将保持原始schema（包含name、age等字段）并发送到主输出表
- 验证失败的数据将被转换为错误表schema（包含source_table_id、source_table_path、original_data、validation_errors、create_time字段）并路由到"error_data"表

### 示例 4: 嵌套规则格式

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    row_error_handle_way = "FAIL"
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

### 示例 5: 使用内置UDF进行邮箱验证

```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    row_error_handle_way = "FAIL"
    field_rules = [
      {
        field_name = "email"
        rule_type = "UDF"
        function_name = "EMAIL"
        custom_message = "邮箱地址格式无效"
      }
    ]
  }
}
```

## UDF开发指南

### 创建自定义UDF函数

要创建自定义验证UDF函数，请按照以下步骤：

#### 1. 实现DataValidatorUDF接口

```java
package com.example.validator;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.validator.ValidationContext;
import org.apache.seatunnel.transform.validator.ValidationResult;
import org.apache.seatunnel.transform.validator.udf.DataValidatorUDF;
import com.google.auto.service.AutoService;

@AutoService(DataValidatorUDF.class)
public class PhoneValidator implements DataValidatorUDF {

    @Override
    public String functionName() {
        return "PHONE_VALIDATOR";
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {

        if (value == null) {
            return ValidationResult.success();
        }

        String phone = value.toString().trim();

        // 自定义手机号验证逻辑
        if (phone.matches("^\\+?[1-9]\\d{1,14}$")) {
            return ValidationResult.success();
        } else {
            return ValidationResult.failure("手机号码格式无效: " + phone);
        }
    }

    @Override
    public String getDescription() {
        return "验证国际手机号码格式";
    }
}
```

#### 2. 注册UDF

UDF通过 `@AutoService(DataValidatorUDF.class)` 注解自动注册。这使用Java的ServiceLoader机制在运行时发现和加载UDF实现。

#### 3. 打包和部署

1. 编译您的UDF类并将其打包到JAR文件中
2. 将JAR文件放置在SeaTunnel类路径中
3. UDF将被自动发现并可供使用

**使用示例**:
```hocon
{
  field_name = "email"
  rule_type = "UDF"
  function_name = "EMAIL"
  custom_message = "请提供有效的邮箱地址"
}
```
