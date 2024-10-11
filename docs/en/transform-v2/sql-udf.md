# SQL UDF

> UDF of SQL transform plugin

## Description

Use UDF SPI to extend the SQL transform functions lib.

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

Add these dependencies and provided scope to your maven project:

```xml

<dependencies>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
        <version>2.3.2</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-api</artifactId>
        <version>2.3.2</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>com.google.auto.service</groupId>
        <artifactId>auto-service</artifactId>
        <version>1.0.1</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

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

Package the UDF project and copy the jar to the path: ${SEATUNNEL_HOME}/lib. And if your UDF use third party library, you also need put it to ${SEATUNNEL_HOME}/lib.  
If you use cluster mode, you need put the lib to all your node's ${SEATUNNEL_HOME}/lib folder and re-start the cluster.

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

