# DataValidator

> Data validation transform plugin

## Description

The DataValidator transform validates field values according to configured rules and handles validation failures based on the specified error handling strategy. It supports multiple validation rule types including null checks, range validation, length validation, and regex pattern matching.

## Options

|      name       |  type  | required | default value |
|-----------------|--------|----------|---------------|
| error_handle_way| enum   | no       | FAIL          |
| error_table     | string | no       |               |
| field_rules     | array  | yes      |               |

### error_handle_way [enum]

Error handling strategy when validation fails:
- `FAIL`: Fail the entire task when validation errors occur
- `SKIP`: Skip invalid rows and continue processing
- `ROUTE_TO_TABLE`: Route invalid data to a specified error table

**Note**: `ROUTE_TO_TABLE` mode only works with sinks that support multiple tables. The sink must be capable of handling data routed to different table destinations.

### error_table [string]

Target table name for routing invalid data when `error_handle_way` is set to `ROUTE_TO_TABLE`. This parameter is required when using `ROUTE_TO_TABLE` mode.

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

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Examples

### Example 1: Basic Validation with FAIL Mode

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

### Example 2: Validation with SKIP Mode

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

### Example 3: Validation with ROUTE_TO_TABLE Mode

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

**Note**: When using `ROUTE_TO_TABLE`, ensure your sink connector supports multiple tables. Valid data will be sent to the main output table, while invalid data will be routed to the specified error table.

### Example 4: Nested Rules Format

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

## Changelog

### new version
- Add DataValidator Transform Connector
- Support NOT_NULL, RANGE, LENGTH, and REGEX validation rules
- Support FAIL, SKIP, and ROUTE_TO_TABLE error handling modes
- Support both flat and nested rule configuration formats
