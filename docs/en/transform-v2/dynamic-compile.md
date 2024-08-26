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

### source_code [string]

The code must implement two methods: getInlineOutputColumns and getInlineOutputFieldValues. getInlineOutputColumns determines the columns you want to add or convert, and the original column structure can be obtained from CatalogTable
GetInlineOutputFieldValues determines your column values. You can fulfill any of your requirements, and even complete RPC requests to obtain new values based on the original columns
If there are third-party dependency packages, please place them in ${SEATUNNEL_HOME}/lib, if you use spark or flink, you need to put it under the libs of the corresponding service.

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

## Example

The data read from source is a table like this:

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

```
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
                          List<Column> columns = new ArrayList<>();
                         PhysicalColumn destColumn =
                         PhysicalColumn.of(
                         "compile_language",
                        BasicType.STRING_TYPE,
                         10,
                        true,
                        "",
                        "");
                         columns.add(destColumn);
                        return columns.toArray(new Column[0]);
                     }
                     public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow) {
                       Object[] fieldValues = new Object[1];
                       fieldValues[0]="GROOVY"
                       return fieldValues;
                     }
                 };"""

  }
}

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

                       ArrayList<Column> columns = new ArrayList<Column>();
                                               PhysicalColumn destColumn =
                                               PhysicalColumn.of(
                                               "compile_language",
                                              BasicType.STRING_TYPE,
                                               10,
                                              true,
                                              "",
                                              "");
                                                 return new Column[]{
                                                                destColumn
                                                        };

                     }
                     public Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow) {
                       Object[] fieldValues = new Object[1];
                       fieldValues[0]="JAVA";
                       return fieldValues;
                     }
                """

  }
 } 
 
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
| Joy Ding | 20  | 123  | GROOVY           |
| May Ding | 20  | 123  | GROOVY           |
| Kin Dom  | 20  | 123  | GROOVY           |
| Joy Dom  | 20  | 123  | GROOVY           |

Then the data in result table `java_out` will like this

|   name   | age | card | compile_language |
|----------|-----|------|------------------|
| Joy Ding | 20  | 123  | JAVA             |
| May Ding | 20  | 123  | JAVA             |
| Kin Dom  | 20  | 123  | JAVA             |
| Joy Dom  | 20  | 123  | JAVA             |

More complex examples can be referred to
https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-transforms-v2-e2e/seatunnel-transforms-v2-e2e-part-2/src/test/resources/dynamic_compile/conf

## Changelog

