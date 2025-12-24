# MaxWell Format

[Maxwell](https://maxwells-daemon.io/) is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL into Kafka, Kinesis and other streaming connectors. Maxwell provides a unified format schema for changelog and supports to serialize messages using JSON.

Seatunnel supports to interpret MaxWell JSON messages as INSERT/UPDATE/DELETE messages into seatunnel system. This is useful in many cases to leverage this feature, such as

        synchronizing incremental data from databases to other systems
        auditing logs
        real-time materialized views on databases
        temporal join changing history of a database table and so on.

Seatunnel also supports to encode the INSERT/UPDATE/DELETE messages in Seatunnel as MaxWell JSON messages, and emit to storage like Kafka. However, currently Seatunnel can’t combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Seatunnel encodes UPDATE_BEFORE and UPDATE_AFTER as DELETE and INSERT MaxWell messages.

# Format Options

|              Option              | Default | Required |                                                                                                 Description                                                                                                  |
|----------------------------------|---------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format                           | (none)  | yes      | Specify what format to use, here should be 'maxwell_json'.                                                                                                                                                   |
| maxwell_json.ignore-parse-errors | false   | no       | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.                                                                                                         |
| maxwell_json.database.include    | (none)  | no       | An optional regular expression to only read the specific databases changelog rows by regular matching the "database" meta field in the MaxWell record. The pattern string is compatible with Java's Pattern. |
| maxwell_json.table.include       | (none)  | no       | An optional regular expression to only read the specific tables changelog rows by regular matching the "table" meta field in the MaxWell record. The pattern string is compatible with Java's Pattern.       |

# How To Use MaxWell format

## Kafka Uses Example

MaxWell provides a unified format for changelog, here is a simple example for an update operation captured from a MySQL products table:

```bash
{
    "database":"test",
    "table":"product",
    "type":"insert",
    "ts":1596684904,
    "xid":7201,
    "commit":true,
    "data":{
        "id":111,
        "name":"scooter",
        "description":"Big 2-wheel scooter ",
        "weight":5.18
    },
    "primary_key_columns":[
        "id"
    ]
}
```

Note: please refer to MaxWell documentation about the meaning of each fields.

The MySQL products table has 4 columns (id, name, description and weight).
The above JSON message is an update change event on the products table where the weight value of the row with id = 111 is changed from 5.18 to 5.15.
Assuming the messages have been synchronized to Kafka topic products_binlog, then we can use the following Seatunnel to consume this topic and interpret the change events.

```bash
env {
    execution.parallelism = 1
    job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "products_binlog"
    plugin_output = "kafka_name"
    start_mode = earliest
    schema = {
      fields {
           id = "int"
           name = "string"
           description = "string"
           weight = "string"
      }
    },
    format = maxwell_json
  }

}

transform {
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "consume-binlog"
    format = maxwell_json
  }
}
```

