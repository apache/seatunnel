# Split

> Transform plugin : Split [Flink]

## Description

A string cutting function is defined, which is used to split the specified field in the Sql plugin.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| separator      | string | no       | ,             |
| fields         | array  | yes      | -             |
| common-options | string | no       | -             |

### separator [string]

The specified delimiter, the default is `,`

### fields [array]

The name of each field after split

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](./transform-plugin.md) for details

## Examples

```bash
  # This just created a udf called split
  Split{
    separator = "#"
    fields = ["name","age"]
  }
  # Use the split function (confirm that the fake table exists)
  sql {
    sql = "select * from (select info,split(info) as info_row from fake) t1"
  }
```
