# Ogg Format

[Oracle GoldenGate](https://www.oracle.com/integration/goldengate/) (a.k.a ogg) is a managed service providing a real-time data mesh platform, which uses replication to keep data highly available, and enabling real-time analysis. Customers can design, execute, and monitor their data replication and stream data processing solutions without the need to allocate or manage compute environments. Ogg provides a format schema for changelog and supports to serialize messages using JSON.

Seatunnel supports to interpret Ogg JSON messages as INSERT/UPDATE/DELETE messages into seatunnel system. This is useful in many cases to leverage this feature, such as

        synchronizing incremental data from databases to other systems
        auditing logs
        real-time materialized views on databases
        temporal join changing history of a database table and so on.

Seatunnel also supports to encode the INSERT/UPDATE/DELETE messages in Seatunnel as Ogg JSON messages, and emit to storage like Kafka. However, currently Seatunnel can’t combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Seatunnel encodes UPDATE_BEFORE and UPDATE_AFTER as DELETE and INSERT Ogg messages.

# Format Options

|            Option            | Default | Required |                                                                                                Description                                                                                                 |
|------------------------------|---------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format                       | (none)  | yes      | Specify what format to use, here should be '-json'.                                                                                                                                                        |
| ogg_json.ignore-parse-errors | false   | no       | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.                                                                                                       |
| ogg_json.database.include    | (none)  | no       | An optional regular expression to only read the specific databases changelog rows by regular matching the "database" meta field in the Canal record. The pattern string is compatible with Java's Pattern. |
| ogg_json.table.include       | (none)  | no       | An optional regular expression to only read the specific tables changelog rows by regular matching the "table" meta field in the Canal record. The pattern string is compatible with Java's Pattern.       |

# How to Use Ogg format

## Kafka Uses Example

Ogg provides a unified format for changelog, here is a simple example for an update operation captured from a Oracle products table:

```bash
{
  "before": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.18
  },
  "after": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.15
  },
  "op_type": "U",
  "op_ts": "2020-05-13 15:40:06.000000",
  "current_ts": "2020-05-13 15:40:07.000000",
  "primary_keys": [
    "id"
  ],
  "pos": "00000000000000000000143",
  "table": "PRODUCTS"
}
```

Note: please refer to [Debezium documentation](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/oracle.adoc#data-change-events) about the meaning of each fields.

The Oracle products table has 4 columns (id, name, description and weight).
The above JSON message is an update change event on the products table where the weight value of the row with id = 111 is changed from 5.18 to 5.15.
Assuming the messages have been synchronized to Kafka topic products_binlog, then we can use the following Seatunnel to consume this topic and interpret the change events.

```bash
env {
    parallelism = 1
    job.mode = "STREAMING"
}
source {
  Kafka {
    bootstrap.servers = "127.0.0.1:9092"
    topic = "ogg"
    plugin_output = "kafka_name"
    start_mode = earliest
    schema = {
      fields {
           id = "int"
           name = "string"
           description = "string"
           weight = "double"
      }
    },
    format = ogg_json
  }
}
sink {
    jdbc {
        url = "jdbc:mysql://127.0.0.1/test"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "12345678"
        table = "ogg"
        primary_keys = ["id"]
    }
}
```

