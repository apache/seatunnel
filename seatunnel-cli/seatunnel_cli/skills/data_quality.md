<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

---
name: data_quality
description: Data quality validation using Assert sink or row-level checks
triggers:
  - assert
  - Assert
  - validate
  - check
  - verify
  - quality
  - test data
  - row count
  - min row
  - max row
  - rule
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants to validate data quality — check row counts, field values, data types,
or use the Assert sink to verify pipeline output meets expectations. Also applies
to FakeSource-based testing scenarios.

## Domain Knowledge

- **Assert** sink validates data against rules and fails the job if rules are not met.
- Assert rules are defined in a `rules { }` block with:
  - `row_rules`: validate row count (`MIN_ROW`, `MAX_ROW`)
  - `field_rules`: validate field values (`NOT_NULL`, `MIN`, `MAX`, `MIN_LENGTH`, `MAX_LENGTH`)
- Assert is often paired with FakeSource for testing transforms, or with real sources for data quality gates.
- **FakeSource** generates synthetic test data with configurable schema:
  - `tables_configs` array with `row.num` and `schema { fields { ... } }`
  - Useful for testing pipelines without real data sources.
- **Console** sink is used for debug/inspection — prints data to stdout.
- For production data quality, combine a real source with Assert sink to validate before loading to the actual sink.

## SOP

1. **Determine validation goal**: row count check, field validation, or end-to-end pipeline test.
2. **If testing with synthetic data**: use FakeSource with appropriate schema and row.num.
3. **If validating real data**: use the actual source connector.
4. **Configure Assert sink** with appropriate rules:
   - `row_rules` for row count validation
   - `field_rules` for field-level validation
5. **Set routing** via plugin_output/plugin_input.
6. **For dual-sink scenarios** (validate AND load): use multi_pipeline skill to route data to both Assert and the real sink.
7. **Validate**: Assert rules syntax correct, field names match source schema.

## Constraints

- Assert `rule_type` values are: `NOT_NULL`, `MIN_ROW`, `MAX_ROW`, `MIN`, `MAX`, `MIN_LENGTH`, `MAX_LENGTH`.
- `row_rules` go in `rules { row_rules = [...] }`, `field_rules` go in `rules { field_rules = { <field> = [...] } }`.
- FakeSource schema field types: `string`, `int`, `bigint`, `float`, `double`, `boolean`, `date`, `timestamp`.
- FakeSource uses `tables_configs` (plural with underscore) — not `table_config`.

## Pattern

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        row.num = <row_count>
        schema {
          fields {
            <field_name> = "<type>"
          }
        }
      }
    ]
    plugin_output = "<routing_label>"
  }
}

sink {
  Assert {
    plugin_input = "<routing_label>"
    rules {
      row_rules = [
        {
          rule_type = MIN_ROW
          rule_value = <expected_min>
        }
      ]
    }
  }
}
```
