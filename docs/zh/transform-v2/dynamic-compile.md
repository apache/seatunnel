# DynamicCompile

> 动态编译插件

## 描述

:::tip

特别申明
您需要确保服务的安全性，并防止攻击者上传破坏性代码

:::

提供一种可编程的方式来处理行，允许用户自定义任何业务行为，甚至基于现有行字段作为参数的RPC请求，或者通过从其他数据源检索相关数据来扩展字段。为了区分业务，您还可以定义多个转换进行组合，
如果转换过于复杂，可能会影响性能

## 属性

|       name       |  type  | required | default value |
|------------------|--------|----------|---------------|
| source_code      | string | no       |               |
| compile_language | Enum   | yes      |               |
| compile_pattern  | Enum   | no       | SOURCE_CODE   |
| absolute_path    | string | no       |               |

### source_code [string]

代码必须实现两个方法：getInlineOutputColumns和getInlineOutputFieldValues。getInlineOutputColumns确定要添加或转换的列，原始列结构可以从CatalogTable中获得
GetInlineOutputFieldValues决定您的列值。您可以满足任何要求，甚至可以完成RPC请求以基于原始列获取新值
如果有第三方依赖包，请将它们放在${SEATUNNEL_HOME}/lib中，如果您使用spark或flink，则需要将其放在相应服务的libs下。

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情。

### compile_language [Enum]

Java中的某些语法可能不受支持，请参阅https://github.com/janino-compiler/janino
GROOVY，JAVA

### compile_pattern [Enum]

SOURCE_CODE,ABSOLUTE_PATH
选择 SOURCE_CODE，SOURCE_CODE 属性必填;选择ABSOLUTE_PATH，ABSOLUTE_PATH属性必填。

### absolute_path [string]

服务器上Java或Groovy文件的绝对路径

## Example

源端数据读取的表格如下：

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

那么结果表 `groovy_out` 中的数据将会更新为：

|   name   | age | card | compile_language |
|----------|-----|------|------------------|
| Joy Ding | 20  | 123  | GROOVY           |
| May Ding | 20  | 123  | GROOVY           |
| Kin Dom  | 20  | 123  | GROOVY           |
| Joy Dom  | 20  | 123  | GROOVY           |

那么结果表 `java_out` 中的数据将会更新为：

|   name   | age | card | compile_language |
|----------|-----|------|------------------|
| Joy Ding | 20  | 123  | JAVA             |
| May Ding | 20  | 123  | JAVA             |
| Kin Dom  | 20  | 123  | JAVA             |
| Joy Dom  | 20  | 123  | JAVA             |

更多复杂例子可以参考
https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-transforms-v2-e2e/seatunnel-transforms-v2-e2e-part-2/src/test/resources/dynamic_compile/conf

## Changelog

