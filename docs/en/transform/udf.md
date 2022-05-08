# udf

## Description

Supports using UDF in data integration by the transform.
Need to specify the function name and class name and put UDF jars in Flink's classpath or import them via 'Flink run -c xxx.jar'

:::tip

This transform **ONLY** supported by Flink.

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| class_names    | string | yes      | -             |
| function_names | string | yes      | -             |

### class_names [string-list]

The names of UDF classes. 

### function_names [string]

The names of UDF which you want to use as.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

Use `udf` in sql.

```bash
  udf {
    class_names = ["com.example.udf.flink.TestUDF"]
    function_names = ["test_1"]
  }
  
  # Use the specify function (confirm that the fake table exists)
  sql {
    sql = "select test_1(name), age from fake"
  }
```
