# DataValidator UDF Examples

This document provides examples of how to use User-Defined Functions (UDFs) with the DataValidator transform.

## Overview

DataValidator UDFs allow you to implement custom validation logic for field values. UDFs are automatically discovered using the `@AutoService` mechanism and can be referenced by name in the configuration.

## Available UDFs

### 1. EMAIL_VALIDATOR

Comprehensive email validation including:
- RFC 5322 compliant format validation
- Domain validation
- Length validation (local part ≤ 64 chars, domain ≤ 253 chars, total ≤ 254 chars)
- Special character handling
- Test/example domain rejection

**Function Name:** `EMAIL_VALIDATOR`

**Usage Example:**
```hocon
transform {
  DataValidator {
    plugin_input = "source"
    plugin_output = "validated"
    error_handle_way = "SKIP"
    field_rules = [
      {
        field_name = "email"
        rule_type = "UDF"
        function_name = "EMAIL_VALIDATOR"
        custom_message = "Invalid email format"
      }
    ]
  }
}
```

**Validation Rules:**
- ✅ `user@example.com` - Valid format
- ❌ `user@test.com` - Test domain not allowed
- ❌ `user..name@example.com` - Consecutive dots
- ❌ `.user@example.com` - Leading dot in local part
- ❌ `user@domain-.com` - Trailing hyphen in domain
- ❌ `user@domain.123` - Numeric TLD

### 2. COMPANY_EMAIL_CHECK

Validates that email addresses are from specific company domains.

**Function Name:** `COMPANY_EMAIL_CHECK`

**Usage Example:**
```hocon
{
  field_name = "work_email"
  rule_type = "UDF"
  function_name = "COMPANY_EMAIL_CHECK"
  custom_message = "Email must be from company domain"
}
```

### 3. EXACT_LENGTH_10

Validates that string values have exactly 10 characters.

**Function Name:** `EXACT_LENGTH_10`

**Usage Example:**
```hocon
{
  field_name = "product_code"
  rule_type = "UDF"
  function_name = "EXACT_LENGTH_10"
  custom_message = "Product code must be exactly 10 characters"
}
```

## Creating Custom UDFs

To create your own UDF:

1. **Implement the DataValidatorUDF interface:**

```java
@AutoService(DataValidatorUDF.class)
public class MyCustomValidator implements DataValidatorUDF {
    
    @Override
    public String functionName() {
        return "MY_CUSTOM_VALIDATOR";
    }
    
    @Override
    public ValidationResult validate(Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        // Your validation logic here
        if (/* validation passes */) {
            return ValidationResult.success();
        } else {
            return ValidationResult.failure("Validation failed: " + value);
        }
    }
    
    @Override
    public String getDescription() {
        return "My custom validation logic";
    }
}
```

2. **Use the @AutoService annotation** to enable automatic discovery.

3. **Reference in configuration:**

```hocon
{
  field_name = "my_field"
  rule_type = "UDF"
  function_name = "MY_CUSTOM_VALIDATOR"
  custom_message = "Custom validation failed"
}
```

## Configuration Options

### UDF Rule Configuration

```hocon
{
  field_name = "field_to_validate"     # Required: Field name to validate
  rule_type = "UDF"                    # Required: Must be "UDF"
  function_name = "UDF_FUNCTION_NAME"  # Required: Name of the UDF function
  custom_message = "Error message"     # Optional: Custom error message
}
```

### Error Handling

UDFs respect the same error handling modes as other validation rules:

- `FAIL` - Stop processing and fail the job
- `SKIP` - Skip invalid rows and continue
- `ROUTE_TO_TABLE` - Route invalid data to error table

## Best Practices

1. **Keep UDFs focused** - Each UDF should validate one specific aspect
2. **Handle null values** - Always check for null input values
3. **Provide meaningful error messages** - Help users understand validation failures
4. **Use custom messages** - Override default error messages in configuration
5. **Test thoroughly** - Validate edge cases and error conditions

## Complete Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        id = "int"
        email = "string"
        code = "string"
      }
    }
    rows = [
      {fields = [1, "user@company.com", "ABC1234567"], kind = INSERT}
      {fields = [2, "invalid-email", "SHORT"], kind = INSERT}
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    error_handle_way = "SKIP"
    field_rules = [
      {
        field_name = "email"
        rule_type = "UDF"
        function_name = "EMAIL_VALIDATOR"
        custom_message = "Invalid email format"
      },
      {
        field_name = "code"
        rule_type = "UDF"
        function_name = "EXACT_LENGTH_10"
        custom_message = "Code must be exactly 10 characters"
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```
