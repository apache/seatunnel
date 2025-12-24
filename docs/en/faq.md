# FAQ

## What data sources and destinations does SeaTunnel support?
SeaTunnel supports various data sources and destinations. You can find a detailed list on the following list:
- Supported data sources (Source): [Source List](https://seatunnel.apache.org/docs/connector-v2/source)
- Supported data destinations (Sink): [Sink List](https://seatunnel.apache.org/docs/connector-v2/sink)

## Does SeaTunnel support batch and streaming processing?
SeaTunnel supports both batch and streaming processing modes. You can select the appropriate mode based on your specific business scenarios and needs. Batch processing is suitable for scheduled data integration tasks, while streaming processing is ideal for real-time integration and Change Data Capture (CDC).

## Is it necessary to install engines like Spark or Flink when using SeaTunnel?
Spark and Flink are not mandatory. SeaTunnel supports Zeta, Spark, and Flink as integration engines, allowing you to choose one based on your needs. The community highly recommends Zeta, a new generation high-performance integration engine specifically designed for integration scenarios. Zeta is affectionately called "Ultraman Zeta" by community users! The community offers extensive support for Zeta, making it the most feature-rich option.

## What data transformation functions does SeaTunnel provide?
SeaTunnel supports multiple data transformation functions, including field mapping, data filtering, data format conversion, and more. You can implement data transformations through the `transform` module in the configuration file. For more details, refer to the SeaTunnel [Transform Documentation](https://seatunnel.apache.org/docs/transform-v2).

## Can SeaTunnel support custom data cleansing rules?
Yes, SeaTunnel supports custom data cleansing rules. You can configure custom rules in the `transform` module, such as cleaning up dirty data, removing invalid records, or converting fields.

## Does SeaTunnel support real-time incremental integration?
SeaTunnel supports incremental data integration. For example, the CDC connector allows real-time capture of data changes, which is ideal for scenarios requiring real-time data integration.

## What CDC data sources are currently supported by SeaTunnel?
SeaTunnel currently supports MongoDB CDC, MySQL CDC, OpenGauss CDC, Oracle CDC, PostgreSQL CDC, SQL Server CDC, TiDB CDC, and more. For more details, refer to the [Source List](https://seatunnel.apache.org/docs/connector-v2/source).

## How do I enable permissions required for SeaTunnel CDC integration?
Please refer to the official SeaTunnel documentation for the necessary steps to enable permissions for each connector’s CDC functionality.

## Does SeaTunnel support CDC from MySQL replicas? How are logs pulled?
Yes, SeaTunnel supports CDC from MySQL replicas by subscribing to binlog logs, which are then parsed on the SeaTunnel server.

## Does SeaTunnel support CDC integration for tables without primary keys?
SeaTunnel does not support CDC integration for tables without primary keys. The reason is that if two identical records exist in the upstream and one is deleted or modified, the downstream cannot determine which record to delete or modify, leading to potential issues. Primary keys are essential to ensure data uniqueness.

## Does SeaTunnel support automatic table creation?
Before starting an integration task, you can select different handling schemes for existing table structures on the target side, controlled via the `schema_save_mode` parameter. Available options include:
- **`RECREATE_SCHEMA`**: Creates the table if it does not exist; if the table exists, it is deleted and recreated.
- **`CREATE_SCHEMA_WHEN_NOT_EXIST`**: Creates the table if it does not exist; skips creation if the table already exists.
- **`ERROR_WHEN_SCHEMA_NOT_EXIST`**: Throws an error if the table does not exist.
- **`IGNORE`**: Ignores table handling.
  Many connectors currently support automatic table creation. Refer to the specific connector documentation, such as [Jdbc sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc/#schema_save_mode-enum), for more information.

## Does SeaTunnel support handling existing data before starting a data integration task?
Yes, you can specify different processing schemes for existing data on the target side before starting an integration task, controlled via the `data_save_mode` parameter. Available options include:
- **`DROP_DATA`**: Retains the database structure but deletes the data.
- **`APPEND_DATA`**: Retains both the database structure and data.
- **`CUSTOM_PROCESSING`**: User-defined processing.
- **`ERROR_WHEN_DATA_EXISTS`**: Throws an error if data already exists.
  Many connectors support handling existing data; please refer to the respective connector documentation, such as [Jdbc sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc#data_save_mode-enum).

## Does SeaTunnel support exactly-once consistency?
SeaTunnel supports exactly-once consistency for some data sources, such as MySQL and PostgreSQL, ensuring data consistency during integration. Note that exactly-once consistency depends on the capabilities of the underlying database.

## Can SeaTunnel execute scheduled tasks?
You can use Linux cron jobs to achieve periodic data integration, or leverage scheduling tools like Apache DolphinScheduler or Apache Airflow to manage complex scheduled tasks.

## I encountered an issue with SeaTunnel that I cannot resolve. What should I do?
If you encounter issues with SeaTunnel, here are a few ways to get help:
1. Search the [Issue List](https://github.com/apache/seatunnel/issues) or [Mailing List](https://lists.apache.org/list.html?dev@seatunnel.apache.org) to see if someone else has faced a similar issue.
2. If you cannot find an answer, reach out to the community through [these methods](https://github.com/apache/seatunnel#contact-us).

## How do I declare variables?
Would you like to declare a variable in SeaTunnel's configuration and dynamically replace it at runtime? This feature is commonly used in both scheduled and ad-hoc offline processing to replace time, date, or other variables. Here's an example:

Define the variable in the configuration. For example, in an SQL transformation (the value in any "key = value" pair in the configuration file can be replaced with variables):

```plaintext
...
transform {
  Sql {
    query = "select * from dual where city ='${city}' and dt = '${date}'"
  }
}
...
```

To start SeaTunnel in Zeta Local mode with variables:

```bash
$SEATUNNEL_HOME/bin/seatunnel.sh \
-c $SEATUNNEL_HOME/config/your_app.conf \
-m local[2] \
-i city=Singapore \
-i date=20231110
```

Use the `-i` or `--variable` parameter with `key=value` to specify the variable's value, where `key` matches the variable name in the configuration. For details, see: [SeaTunnel Variable Configuration](https://seatunnel.apache.org/docs/concept/config)

## How can I write multi-line text in the configuration file?
If the text is long and needs to be wrapped, you can use triple quotes to indicate the beginning and end:

```plaintext
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
```

## How do I perform variable substitution in multi-line text?
Performing variable substitution in multi-line text can be tricky because variables cannot be enclosed within triple quotes:

```plaintext
var = """
your string 1
"""${your_var}""" your string 2"""
```

For more details, see: [lightbend/config#456](https://github.com/lightbend/config/issues/456).


## Where should I start if I want to learn SeaTunnel source code?
SeaTunnel features a highly abstracted and well-structured architecture, making it an excellent choice for learning big data architecture. You can start by exploring and debugging the `seatunnel-examples` module: `SeaTunnelEngineLocalExample.java`. For more details, refer to the [SeaTunnel Contribution Guide](https://seatunnel.apache.org/docs/contribution/setup).

## Do I need to understand all of SeaTunnel’s source code if I want to develop my own source, sink, or transform?
No, you only need to focus on the interfaces for source, sink, and transform. If you want to develop your own connector (Connector V2) for the SeaTunnel API, refer to the **[Connector Development Guide](https://github.com/apache/seatunnel/blob/dev/seatunnel-connectors-v2/README.md)**.
