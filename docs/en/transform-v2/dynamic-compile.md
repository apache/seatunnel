# DynamicCompile

> DynamicCompile transform plugin

## Description

:::tip

important clause
You need to ensure the security of your service and prevent attackers from uploading destructive code

:::

Provide a programmable way to process rows, allowing users to customize any business behavior, even RPC requests based on existing row fields as parameters, or to expand fields by retrieving associated data from other data sources. To distinguish businesses, you can also define multiple transforms to combine,
If the conversion is too complex, it may affect performance

## Options

|       name       |  type  | required | default value |
|------------------|--------|----------|---------------|
| source_code      | string | no       |               |
| compile_language | Enum   | yes      |               |
| compile_pattern  | Enum   | no       | SOURCE_CODE   |
| absolute_path    | string | no       |               |


### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

### compile_language [Enum]

Some syntax in Java may not be supported, please refer https://github.com/janino-compiler/janino
GROOVY,JAVA

### compile_pattern [Enum]

SOURCE_CODE,ABSOLUTE_PATH
If it is a SOURCE-CODE enumeration; the SOURCE-CODE attribute is required, and the ABSOLUTE_PATH enumeration;ABSOLUTE_PATH attribute is required

### absolute_path [string]

The absolute path of Java or Groovy files on the server

### source_code [string]

The source code.

#### Details about the source code

In the source code, you must implement two method:
- `Column[] getInlineOutputColumns(CatalogTable inputCatalogTable)`  
- `Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow)`

`getInlineOutputColumns` method, input parameter is `CatalogTable`, return type is `Column[]`.   
you can get the current table's schema from `CatalogTable`.  
if the return column exist in current schema, then it will overwrite by returned value (field type, comment, ...), if it's a new column, it will add into current schema.

`getInlineOutputFieldValues` method, input parameter is `SeaTunnelRowAccessor`, return type is `Object[]`
You can get the record from `SeaTunnelRowAccessor`, do you own customized data process logical.  
The return `Object[]` array length should match with `getInlineOutputColumns` method result's length. and the order also need be match.   

If there are third-party dependency packages, please place them in ${SEATUNNEL_HOME}/lib, if you use spark or flink, you need to put it under the libs of the corresponding service. 
You need restart the server to load the lib file.


## Example

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 30  | 123  |
| Joy Dom  | 30  | 123  |

Use this DynamicCompile to add a new column `compile_language`, and update the `age` field by its original value (if age = 20, update to 40)


- use groovy
```hacon
transform {
 DynamicCompile {
    source_table_name = "fake"
    result_table_name = "groovy_out"
    compile_language="GROOVY"
    compile_pattern="SOURCE_CODE"
    source_code="""
                 import org.apache.seatunnel.api.table.catalog.Column
                 import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor
                 import org.apache.seatunnel.api.table.catalog.CatalogTable
                 import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
                 import org.apache.seatunnel.api.table.type.*;
                 import java.util.ArrayList;
                 class demo  {
                    public Column[] getInlineOutputColumns(CatalogTable inputCatalogTable) {
                        PhysicalColumn col1 =
                                PhysicalColumn.of(
                                        "compile_language",
                                        BasicType.STRING_TYPE,
                                        10L,
                                        true,
                                        "",
                                        "");
                        PhysicalColumn col2 =
                                PhysicalColumn.of(
                                        "age",
                                        BasicType.INT_TYPE,
                                        0L,
                                        false,
                                        false,
                                        ""
                                );
                        return new Column[]{
                                col1, col2
                        };
                    }
                
                
                    public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow) {
                        Object[] fieldValues = new Object[2];
                        // get age 
                        Object ageField = inputRow.getField(1);
                        fieldValues[0] = "GROOVY";
                        if (Integer.parseInt(ageField.toString()) == 20) {
                            fieldValues[1] = 40;
                        } else {
                            fieldValues[1] = ageField;
                        }
                        return fieldValues;
                    }
                 };"""

  }
}
```

- use java 
```hacon
transform {
 DynamicCompile {
    source_table_name = "fake"
    result_table_name = "java_out"
    compile_language="JAVA"
    compile_pattern="SOURCE_CODE"
    source_code="""
                 import org.apache.seatunnel.api.table.catalog.Column;
                 import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
                 import org.apache.seatunnel.api.table.catalog.*;
                 import org.apache.seatunnel.api.table.type.*;
                 import java.util.ArrayList;
                    public Column[] getInlineOutputColumns(CatalogTable inputCatalogTable) {
                        PhysicalColumn col1 =
                                PhysicalColumn.of(
                                        "compile_language",
                                        BasicType.STRING_TYPE,
                                        10L,
                                        true,
                                        "",
                                        "");
                        PhysicalColumn col2 =
                                PhysicalColumn.of(
                                        "age",
                                        BasicType.INT_TYPE,
                                        0L,
                                        false,
                                        false,
                                        ""
                                );
                        return new Column[]{
                                col1, col2
                        };
                    }
                
                
                    public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow) {
                        Object[] fieldValues = new Object[2];
                        // get age 
                        Object ageField = inputRow.getField(1);
                        fieldValues[0] = "JAVA";
                        if (Integer.parseInt(ageField.toString()) == 20) {
                            fieldValues[1] = 40;
                        } else {
                            fieldValues[1] = ageField;
                        }
                        return fieldValues;
                    }
                """

  }
 } 
 ```
- use absolute path to read code
```hacon
 transform {
 DynamicCompile {
    source_table_name = "fake"
    result_table_name = "groovy_out"
    compile_language="GROOVY"
    compile_pattern="ABSOLUTE_PATH"
    absolute_path="""/tmp/GroovyFile"""

  }
}
```

Then the data in result table `groovy_out` will like this

|   name   | age | card | compile_language | 
|----------|-----|------|------------------|
| Joy Ding | 40  | 123  | GROOVY           |
| May Ding | 40  | 123  | GROOVY           |
| Kin Dom  | 30  | 123  | GROOVY           |
| Joy Dom  | 30  | 123  | GROOVY           |

Then the data in result table `java_out` will like this

|   name   | age | card | compile_language |
|----------|-----|------|------------------|
| Joy Ding | 40  | 123  | JAVA             |
| May Ding | 40  | 123  | JAVA             |
| Kin Dom  | 30  | 123  | JAVA             | 
| Joy Dom  | 30  | 123  | JAVA             |

More complex examples can be referred to
https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-transforms-v2-e2e/seatunnel-transforms-v2-e2e-part-2/src/test/resources/dynamic_compile/conf

## Changelog

