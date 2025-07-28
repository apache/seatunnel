# DataValidator

> Data validation transform plugin

## Description

The DataValidator transform validates field values according to configured rules and handles validation failures based on the specified error handling strategy. It supports multiple validation rule types including null checks, range validation, length validation, and regex pattern matching.

## Options

|      name       |  type  | required | default value |
|-----------------|--------|----------|---------------|
| row_error_handle_way| enum   | no       | FAIL          |
| row_error_handle_way.error_table     | string | no       |               |
| field_rules     | array  | yes      |               |

### row_error_handle_way [enum]

Error handling strategy when validation fails:
- `FAIL`: Fail the entire task when validation errors occur
- `SKIP`: Skip invalid rows and continue processing
- `ROUTE_TO_TABLE`: Route invalid data to a specified error table

**Note**: `ROUTE_TO_TABLE` mode only works with sinks that support multiple tables. The sink must be capable of handling data routed to different table destinations.

### row_error_handle_way.error_table [string]

Target table name for routing invalid data when `row_error_handle_way` is set to `ROUTE_TO_TABLE`. This parameter is required when using `ROUTE_TO_TABLE` mode.

#### Error Table Schema

When using `ROUTE_TO_TABLE` mode, DataValidator automatically creates an error table with a fixed schema to store validation failure data. The error table contains the following fields:

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| source_table_id | STRING | Source table identifier that identifies the originating table |
| source_table_path | STRING | Source table path with complete table path information |
| original_data | STRING | JSON representation of the original data containing the complete row that failed validation |
| validation_errors | STRING | JSON array of validation error details containing all failed fields and error information |
| create_time | TIMESTAMP | Creation time of the validation error |

**Complete Error Table Record Example**:
```json
{
  "source_table_id": "users_table",
  "source_table_path": "database.users",
  "original_data": "{\"id\": 123, \"name\": null, \"age\": 200, \"email\": \"invalid-email\"}",
  "validation_errors": "[{\"field_name\": \"name\", \"error_message\": \"Field 'name' cannot be null\"}, {\"field_name\": \"age\", \"error_message\": \"Field 'age' value 200 is not within range [0, 150]\"}, {\"field_name\": \"email\", \"error_message\": \"Field 'email' does not match pattern '^[\\\\w-\\\\.]+@([\\\\w-]+\\\\.)+[\\\\w-]{2,4}$'\"}]",
  "create_time": "2024-01-15T10:30:45"
}
```

**Data Routing Mechanism**:
- Data that passes validation maintains the original schema and is routed to the main output table
- Data that fails validation is converted to the error table schema format above and routed to the specified error table
- Each validation failure row generates one record in the error table, containing complete original data and detailed error information

### field_rules [array]

Array of field validation rules. Each rule defines validation criteria for a specific field.

#### Field Rule Structure

Each field rule contains:
- `field_name`: Name of the field to validate
- `rules`: Array of validation rules to apply (nested format), or individual rule properties (flat format)

#### Validation Rule Types

##### NOT_NULL
Validates that a field value is not null.

Parameters:
- `rule_type`: "NOT_NULL"
- `custom_message` (optional): Custom error message

##### RANGE
Validates that a numeric value is within a specified range.

Parameters:
- `rule_type`: "RANGE"
- `min_value` (optional): Minimum allowed value
- `max_value` (optional): Maximum allowed value
- `min_inclusive` (optional): Whether minimum value is inclusive (default: true)
- `max_inclusive` (optional): Whether maximum value is inclusive (default: true)
- `custom_message` (optional): Custom error message

##### LENGTH
Validates the length of string, array, or collection values.

Parameters:
- `rule_type`: "LENGTH"
- `min_length` (optional): Minimum allowed length
- `max_length` (optional): Maximum allowed length
- `exact_length` (optional): Exact required length
- `custom_message` (optional): Custom error message

##### REGEX
Validates that a string value matches a regular expression pattern.

Parameters:
- `rule_type`: "REGEX"
- `pattern`: Regular expression pattern (required)
- `case_sensitive` (optional): Whether pattern matching is case sensitive (default: true)
- `custom_message` (optional): Custom error message

##### UDF (User Defined Function)
Validates field values using custom business logic implemented as a User Defined Function.

Parameters:
- `rule_type`: "UDF"
- `function_name`: Name of the UDF function to execute (required)
- `custom_message` (optional): Custom error message

**Built-in UDF Functions:**
- `EMAIL`: Validates email addresses using practical validation rules based on OWASP recommendations

**Creating Custom UDF Functions:**
To create a custom UDF function:
1. Implement the `DataValidatorUDF` interface
2. Use `@AutoService(DataValidatorUDF.class)` annotation
3. Provide a unique `functionName()`
4. Implement the `validate()` method with your custom logic

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Examples

### Example 1: Basic Validation with FAIL Mode

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

### Example 2: Validation with SKIP Mode

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

### Example 3: Validation with ROUTE_TO_TABLE Mode

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

**Note**: When using `ROUTE_TO_TABLE`, ensure your sink connector supports multiple tables. Valid data will be sent to the main output table, while invalid data will be routed to the specified error table.

In this example:
- Data that passes validation will maintain the original schema (containing name, age, etc. fields) and be sent to the main output table
- Data that fails validation will be converted to the error table schema (containing source_table_id, source_table_path, original_data, validation_errors, create_time fields) and routed to the "error_data" table

### Example 4: Nested Rules Format

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
            custom_message = "Name is required"
          },
          {
            rule_type = "LENGTH"
            min_length = 2
            max_length = 50
            custom_message = "Name must be between 2 and 50 characters"
          }
        ]
      }
    ]
  }
}
```

### Example 5: Email Validation using Built-in UDF

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
        custom_message = "Invalid email address format"
      }
    ]
  }
}
```

## UDF Development Guide

### Creating Custom UDF Functions

To create a custom validation UDF function, follow these steps:

#### 1. Implement the DataValidatorUDF Interface

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

        // Custom phone validation logic
        if (phone.matches("^\\+?[1-9]\\d{1,14}$")) {
            return ValidationResult.success();
        } else {
            return ValidationResult.failure("Invalid phone number format: " + phone);
        }
    }

    @Override
    public String getDescription() {
        return "Validates international phone number format";
    }
}
```

#### 2. Register the UDF

The UDF is automatically registered using the `@AutoService(DataValidatorUDF.class)` annotation. This uses Java's ServiceLoader mechanism to discover and load UDF implementations at runtime.

#### 3. Package and Deploy

1. Compile your UDF class and package it into a JAR file
2. Place the JAR file in the SeaTunnel classpath
3. The UDF will be automatically discovered and available for use


**Usage Example**:
```hocon
{
  field_name = "email"
  rule_type = "UDF"
  function_name = "EMAIL"
  custom_message = "Please provide a valid email address"
}
```