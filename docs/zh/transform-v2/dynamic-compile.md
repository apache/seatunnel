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

### source_code [string]
源代码

#### 关于源代码
在代码中，你必须实现两个方法
- `Column[] getInlineOutputColumns(CatalogTable inputCatalogTable)`
- `Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow)`

`getInlineOutputColumns`方法中，入参类型为`CatalogTable`，返回结果为`Column[]`。
你可以从入参的`CatalogTable`获取当前表的表结构。
在返回结果中，如果字段已经存在，则会根据返回结果进行覆盖，如果不存在，则会添加到现有表结构中。 

`getInlineOutputFieldValues`方法，入参类型为`SeaTunnelRowAccessor`，返回结果为`Object[]`
你可以从`SeaTunnelRowAccessor`获取到当前行的数据，进行自己的定制化数据处理逻辑。
返回结果中，数组长度需要与`getInlineOutputColumns`方法返回的长度一致，并且里面的字段值顺序也需要保持一致。

如果有第三方依赖包，请将它们放在${SEATUNNEL_HOME}/lib中，如果您使用spark或flink，则需要将其放在相应服务的libs下。
你需要重启集群服务，才能重新加载这些依赖。


## Example

源端数据读取的表格如下：

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 30  | 123  |
| Joy Dom  | 30  | 123  |

我们将使用`DynamicCompile`对数据进行修改，添加一列`compile_language`字段，并且将`age`字段更新，当`age=20`时将其更新为`40`

- 使用groovy
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

- 使用java
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
- 指定源码文件路径
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

那么结果表 `groovy_out` 中的数据将会更新为：

|   name   | age | card | compile_language |
|----------|-----|------|------------------|
| Joy Ding | 40  | 123  | GROOVY           |
| May Ding | 40  | 123  | GROOVY           |
| Kin Dom  | 30  | 123  | GROOVY           |
| Joy Dom  | 30  | 123  | GROOVY           |

那么结果表 `java_out` 中的数据将会更新为：

|   name   | age | card | compile_language |
|----------|-----|------|------------------|
| Joy Ding | 40  | 123  | JAVA             |
| May Ding | 40  | 123  | JAVA             |
| Kin Dom  | 30  | 123  | JAVA             |
| Joy Dom  | 30  | 123  | JAVA             |

更多复杂例子可以参考
https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-transforms-v2-e2e/seatunnel-transforms-v2-e2e-part-2/src/test/resources/dynamic_compile/conf

## Changelog

