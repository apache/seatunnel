# RowKindExtractor

> RowKindExtractor transform plugin

## Description

The RowKindExtractor transform plugin is used to convert CDC (Change Data Capture) data streams into Append-Only mode while extracting the original RowKind information as a new field.

**Core Features:**
- Converts all data rows' RowKind to `+I` (INSERT), achieving Append-Only mode
- Saves the original RowKind information (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) to a newly added field
- Supports both short format and full format output

**Why is this plugin needed?**

In CDC data synchronization scenarios, data rows carry RowKind markers (+I, -U, +U, -D) representing different change types. However, some downstream systems (such as data lakes, analytical systems) only support Append-Only mode and do not support UPDATE and DELETE operations. In such cases, you need to:
1. Convert all data to INSERT type (Append-Only)
2. Save the original change type as a regular field for subsequent analysis

**Transformation Example:**

```
Input (CDC data):
  RowKind: -D (DELETE)
  Data: id=1, name="test1", age=20

Output (Append-Only data):
  RowKind: +I (INSERT)
  Data: id=1, name="test1", age=20, row_kind="DELETE"
```

**Typical Use Cases:**
- Writing CDC data to data lakes that only support Append mode
- Preserving complete change history in data warehouses
- Performing statistical analysis on different types of changes

## Options

| name              | type   | required | default value | description |
|-------------------|--------|----------|---------------|-------------|
| custom_field_name | string | no       | row_kind      | The name of the new field used to store the original RowKind information |
| transform_type    | enum   | no       | SHORT         | The output format of RowKind, options: SHORT (short format) or FULL (full format) |

### custom_field_name [string]

Specifies the name of the new field that will store the original RowKind information.

**Default value:** `row_kind`

**Notes:**
- The field name cannot duplicate existing field names, otherwise an error will be thrown
- It's recommended to use meaningful names, such as `operation_type`, `change_type`, `cdc_op`, etc.

**Example:**
```hocon
custom_field_name = "operation_type"  # Use custom field name
```

### transform_type [enum]

Specifies the output format of the RowKind field value.

**Available options:**

| Format | Description | Output Values |
|--------|-------------|---------------|
| SHORT | Short format (symbol representation) | `+I`, `-U`, `+U`, `-D` |
| FULL | Full format (English names) | `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE` |

**Default value:** `SHORT`

**Meaning of each value:**

| RowKind Type | SHORT Format | FULL Format | Description |
|--------------|--------------|-------------|-------------|
| INSERT | +I | INSERT | Insert operation |
| UPDATE_BEFORE | -U | UPDATE_BEFORE | Value before update |
| UPDATE_AFTER | +U | UPDATE_AFTER | Value after update |
| DELETE | -D | DELETE | Delete operation |

**Selection Recommendations:**
- **SHORT format**: Saves storage space, suitable for storage-sensitive scenarios
- **FULL format**: Better readability, suitable for scenarios requiring manual review or analysis

**Example:**
```hocon
transform_type = FULL  # Use full format
```

## Complete Examples

### Example 1: Using Default Configuration (SHORT Format)

Using default configuration to convert CDC data to Append-Only mode, with RowKind saved in short format.

```yaml
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "cdc_source"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["mydb.users"]
    url = "jdbc:mysql://localhost:3306/mydb"
  }
}

transform {
  RowKindExtractor {
    plugin_input = "cdc_source"
    plugin_output = "append_only_data"
    # Using default configuration:
    # custom_field_name = "row_kind"
    # transform_type = SHORT
  }
}

sink {
  Console {
    plugin_input = "append_only_data"
  }
}
```

**Data Transformation Process:**

```
Input data (CDC format):
  1. RowKind=+I, id=1, name="John", age=25
  2. RowKind=-U, id=1, name="John", age=25
  3. RowKind=+U, id=1, name="John", age=26
  4. RowKind=-D, id=1, name="John", age=26

Output data (Append-Only format):
  1. RowKind=+I, id=1, name="John", age=25, row_kind="+I"
  2. RowKind=+I, id=1, name="John", age=25, row_kind="-U"
  3. RowKind=+I, id=1, name="John", age=26, row_kind="+U"
  4. RowKind=+I, id=1, name="John", age=26, row_kind="-D"
```

---

### Example 2: Using FULL Format with Custom Field Name

Using full format to output RowKind with a custom field name.

```yaml
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "cdc_source"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["mydb.orders"]
    url = "jdbc:mysql://localhost:3306/mydb"
  }
}

transform {
  RowKindExtractor {
    plugin_input = "cdc_source"
    plugin_output = "append_only_data"
    custom_field_name = "operation_type"  # Custom field name
    transform_type = FULL                 # Use full format
  }
}

sink {
  Iceberg {
    plugin_input = "append_only_data"
    catalog_name = "iceberg_catalog"
    database = "mydb"
    table = "orders_history"
    # Iceberg table will contain operation_type field, recording the change type of each data row
  }
}
```

**Data Transformation Process:**

```
Input data (CDC format):
  1. RowKind=+I, order_id=1001, amount=100.00
  2. RowKind=-U, order_id=1001, amount=100.00
  3. RowKind=+U, order_id=1001, amount=150.00
  4. RowKind=-D, order_id=1001, amount=150.00

Output data (Append-Only format, FULL format):
  1. RowKind=+I, order_id=1001, amount=100.00, operation_type="INSERT"
  2. RowKind=+I, order_id=1001, amount=100.00, operation_type="UPDATE_BEFORE"
  3. RowKind=+I, order_id=1001, amount=150.00, operation_type="UPDATE_AFTER"
  4. RowKind=+I, order_id=1001, amount=150.00, operation_type="DELETE"
```

---

### Example 3: Complete Test Example (Using FakeSource)

Using FakeSource to generate test data, demonstrating the transformation effects of various RowKinds.

```yaml
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake_cdc_data"
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
      }
      primaryKey {
        name = "pk_id"
        columnNames = [pk_id]
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100]
      },
      {
        kind = INSERT
        fields = [2, "B", 100]
      },
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_updated", 95]
      },
      {
        kind = UPDATE_BEFORE
        fields = [2, "B", 100]
      },
      {
        kind = UPDATE_AFTER
        fields = [2, "B_updated", 98]
      },
      {
        kind = DELETE
        fields = [1, "A_updated", 95]
      }
    ]
  }
}

transform {
  RowKindExtractor {
    plugin_input = "fake_cdc_data"
    plugin_output = "transformed_data"
    custom_field_name = "change_type"
    transform_type = FULL
  }
}

sink {
  Console {
    plugin_input = "transformed_data"
  }
}
```

**Expected Output:**

```
+I, pk_id=1, name="A", score=100, change_type="INSERT"
+I, pk_id=2, name="B", score=100, change_type="INSERT"
+I, pk_id=1, name="A", score=100, change_type="UPDATE_BEFORE"
+I, pk_id=1, name="A_updated", score=95, change_type="UPDATE_AFTER"
+I, pk_id=2, name="B", score=100, change_type="UPDATE_BEFORE"
+I, pk_id=2, name="B_updated", score=98, change_type="UPDATE_AFTER"
+I, pk_id=1, name="A_updated", score=95, change_type="DELETE"
```
