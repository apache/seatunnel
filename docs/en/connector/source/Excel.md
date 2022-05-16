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

For details, refer to the parameters of the third-party library [spark-excel](https://github.com/crealytics/spark-excel). Examples are as follows:

```
options.dataAddress = "My Sheet'!B3:C35" // optionsal, default: "A1"
options.header = true // Required
options.treatEmptyValuesAsNulls = false// optionsal, default: true
options.setErrorCellsToFallbackValues= true // optionsal, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
options.usePlainNumberFormat= false // optionsal, default: false, If true, format the cells without rounding and scientific notations
options.inferSchema= false // optionsal, default: false
options.addColorColumns= true // optionsal, default: false
options.timestampFormat= "MM-dd-yyyy HH:mm:ss" // optionsal, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
options.maxRowsInMemory= 20 // optionsal, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
options.excerptSize= 10 // optionsal, default: 10. If set and if schema inferred, number of rows to infer schema from
options.workbookPassword= "pass" // optionsal, default None. Requires unlimited strength JCE for older JVMs
```



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

