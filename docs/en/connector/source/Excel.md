# Excel

## Description

Read data from excel file

:::tip

Engine Supported and plugin name

* [x] Spark: Excel
* [ ] Flink:

:::

## Options

| name              | type    | required | default value |
| ----------------- | ------- | -------- | ------------- |
| path              | string  | yes      |               |
| options.useHeader | boolean | yes      |               |
| options.*         |         |          |               |

### path[string]

file path, starting with file://

### options.useHeader[boolean]

Whether to use the first row of Excel as the header

### options.*

For details, refer to the parameters of the third-party library [spark-excel](https://github.com/crealytics/spark-excel).

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details.

## Example

```bash
 Excel {
    path = "/opt/log/test.xlsx"
    options.useHeader = true
    result_table_name= "result_sheet"
   }
```

