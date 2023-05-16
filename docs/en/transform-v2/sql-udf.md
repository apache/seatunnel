# SQL UDF

> UDF of SQL transform plugin

## Description

Use UDF SPI to extends the SQL transform functions lib.

## UDF API

```java
package org.apache.seatunnel.transform.sql.zeta;

public interface ZetaUDF {
    /**
     * Function name
     *
     * @return function name
     */
    String functionName();

    /**
     * The type of function result
     *
     * @param argsType input arguments type
     * @return result type
     */
    SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType);

    /**
     * Evaluate
     *
     * @param args input arguments
     * @return result value
     */
    Object evaluate(List<Object> args);
}
```

## UDF Implements Example

Add the dependency of transform-v2 and provided scope to your maven project:

```xml

<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-transforms-v2</artifactId>
    <version>2.3.x</version>
    <scope>provided</scope>
</dependency>
```

Add a Java Class implements of ZetaUDF like this:

```java

@AutoService(ZetaUDF.class)
public class ExampleUDF implements ZetaUDF {
    @Override
    public String functionName() {
        return "EXAMPLE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) return null;
        return "UDF: " + arg;
    }
}
```

Package the UDF project and copy the jar to the path: ${SEATUNNEL_HOME}/lib

## Example

The data read from source is a table like this:

| id |   name   | age |
|----|----------|-----|
| 1  | Joy Ding | 20  |
| 2  | May Ding | 21  |
| 3  | Kin Dom  | 24  |
| 4  | Joy Dom  | 22  |

We use UDF of SQL query to transform the source data like this:

```
transform {
  Sql {
    source_table_name = "fake"
    result_table_name = "fake1"
    query = "select id, example(name) as name, age from fake"
  }
}
```

Then the data in result table `fake1` will update to

| id |     name      | age |
|----|---------------|-----|
| 1  | UDF: Joy Ding | 20  |
| 2  | UDF: May Ding | 21  |
| 3  | UDF: Kin Dom  | 24  |
| 4  | UDF: Joy Dom  | 22  |

## Changelog

### new version

- Add UDF of SQL Transform Connector

