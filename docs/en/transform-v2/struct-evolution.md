# StructEvolution transform plugin

> Dynamically evolve table schema—add, modify, or drop columns (and update primary keys, indexes, partitions, and
> comments) at runtime.

## Description

StructEvolution is a Transform‑V2 plugin that lets you programmatically change the structure of your rows as they flow
through SeaTunnel. You can:

* **ADD** new columns (specify position, data type, nullability, default value, comment)
* **MODIFY** existing columns (rename, move position, change type, adjust nullability, update default value or comment)
* **DROP** unwanted columns

## Options

| name          | type                 | required | default value |
|---------------|----------------------|----------|---------------|
| specific      | List<SpecificModify> | yes      |               |
| plugin_input  | String               | no       | —             |
| plugin_output | String               | no       | —             |

### specific [config]

Define one or more schema‑evolution rules for a logical table or view. Each `SpecificModify` block contains:

| field       | type            | required | description                                                     |
|-------------|-----------------|----------|-----------------------------------------------------------------|
| input_name  | String          | yes      | Source table or view name.                                      |
| output_name | String          | yes      | Target table or view name.                                      |
| columns     | List<Column>    | no       | Column‑level operations (`ADD`, `MODIFY`, `DROP`).              |
| primary_key | Primarykey      | no       | Primary key changes (rename/reorder key columns).               |
| indexes     | List<Index>     | no       | Index operations (add or drop with column list and sort order). |
| partition   | PartitionConfig | no       | Partition configuration changes.                                |
| comment     | Comment         | no       | Table comment change or removal.                                |

#### Column action config

```hocon
columns = [
  {
    action       = "MODIFY"           # ADD, MODIFY, or DROP
    input_name   = "old_col"          # existing column
    output_name  = "new_col"          # target name (for MODIFY or ADD)
    position     = 2                  # zero‑based index of final position
    data_type    = "VARCHAR(50)"      # SQL type (for ADD or MODIFY)
    nullable     = false              # allow NULL?
    default_value= "N/A"              # default (ADD or MODIFY)
    comment      = "Updated field"    # column comment
  },
  { action = "DROP", input_name = "unused_col" },
  { action = "ADD",  input_name = "new_flag", output_name = "new_flag",
    position = 5, data_type = "BOOLEAN", nullable = true, default_value = false }
]
```

### common options [string]

Transform plugin common parameters, please refer
to [Transform Common Options](https://seatunnel.apache.org/docs/transform-v2/common-options/) ([Apache SeaTunnel | Apache SeaTunnel][2]).

## Example

Assume a source table `employees`:

| id | name        | salary | dept  |
|----|-------------|--------|-------|
| 1  | Alice Smith | 70000  | Sales |
| 2  | Bob Lee     | 80000  | Eng   |
| 3  | Carol Yang  | 75000  | HR    |

We want to:

1. Rename `name` → `full_name` and move it to position 1
2. Drop the `dept` column
3. Add a new `join_date` column of type `DATE` at the end (position 3)

```hocon
transform {
  StructEvolution {
    plugin_input  = "employees"
    plugin_output = "employees_v2"
    specific = [
      {
        input_name  = "employees"
        output_name = "employees_v2"
        columns = [
          { action = "MODIFY", input_name = "name", output_name = "full_name", position = 1 },
          { action = "DROP",   input_name = "dept" },
          { action = "ADD",    input_name = "join_date", output_name = "join_date",
            position = 3, data_type = "DATE", nullable = true, default_value = null }
        ]
      }
    ]
  }
}
```

Resulting `employees_v2`:

| id | full_name   | salary | join_date |
|----|-------------|--------|-----------|
| 1  | Alice Smith | 70000  | *null*    |
| 2  | Bob Lee     | 80000  | *null*    |
| 3  | Carol Yang  | 75000  | *null*    |

## Changelog
