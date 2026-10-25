---
slug: /transforms
---

# Transforms Overview

Transforms sit between source and sink. They are used for field mapping, filtering, SQL processing, table routing, and other mid-pipeline operations. New users do not need to read every transform first; come here after you already know what you want to read and where you want to write it.

## Pick A Starting Point

| Goal | Start here |
| --- | --- |
| Understand how transforms connect datasets | [Transform Common Options](./common-options/common-options.md) |
| Filter rows or trim fields | [Filter](./filter.md) and [Field Mapper](./field-mapper.md) |
| Use SQL-style expressions | [SQL](./sql.md) and [SQL Functions](./sql-functions.md) |
| Rename or reshape fields | [Field Rename](./field-rename.md) and [Split](./split.md) |
| Work with multiple tables | [Transform Multi Table](./transform-multi-table.md) and [Table Merge](./table-merge.md) |

## Recommended Order For New Users

1. Read the common options page first so `plugin_input` and `plugin_output` are clear.
2. Choose the simplest transform that matches your goal before moving to SQL or multi-table orchestration.
3. Add transforms one step at a time and keep the pipeline readable while you validate the job.
