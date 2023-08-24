# Debezium Format

Changelog-Data-Capture Format: Serialization Schema Format: Deserialization Schema

Debezium is a set of distributed services to capture changes in your databases so that your applications can see those changes and respond to them. Debezium records all row-level changes within each database table in a *change event stream*, and applications simply read these streams to see the change events in the same order in which they occurred.

Seatunnel supports to interpret Debezium JSON messages as INSERT/UPDATE/DELETE messages into seatunnel system. This is useful in many cases to leverage this feature, such as

        synchronizing incremental data from databases to other systems
        auditing logs
        real-time materialized views on databases
        temporal join changing history of a database table and so on.

Seatunnel also supports to encode the INSERT/UPDATE/DELETE messages in Seatunnel asDebezium JSON messages, and emit to storage like Kafka.

# Format Options

|              option               | default | required |                                             Description                                              |
|-----------------------------------|---------|----------|------------------------------------------------------------------------------------------------------|
| format                            | (none)  | yes      | Specify what format to use, here should be 'debezium_json'.                                          |
| debezium-json.ignore-parse-errors | false   | no       | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors. |

# How to use Debezium format

## Kafka uses example

Debezium provides a unified format for changelog, here is a simple example for an update operation captured from a MySQL products table:

```bash
{
	"before": {
		"id": 111,
		"name": "scooter",
		"description": "Big 2-wheel scooter ",
		"weight": 5.18
	},
	"after": {
		"id": 111,
		"name": "scooter",
		"description": "Big 2-wheel scooter ",
		"weight": 5.17
	},
	"source": {
		"version": "1.1.1.Final",
		"connector": "mysql",
		"name": "dbserver1",
		"ts_ms": 1589362330000,
		"snapshot": "false",
		"db": "inventory",
		"table": "products",
		"server_id": 223344,
		"gtid": null,
		"file": "mysql-bin.000003",
		"pos": 2090,
		"row": 0,
		"thread": 2,
		"query": null
	},
	"op": "u",
	"ts_ms": 1589362330904,
	"transaction": null
}
```

Note: please refer to Debezium documentation about the meaning of each fields.

The MySQL products table has 4 columns (id, name, description and weight).
The above JSON message is an update change event on the products table where the weight value of the row with id = 111 is changed from 5.18 to 5.15.
Assuming the messages have been synchronized to Kafka topic products_binlog, then we can use the following Seatunnel conf to consume this topic and interpret the change events by Debezium format.

```bash
env {
    execution.parallelism = 1
    job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "products_binlog"
    result_table_name = "kafka_name"
    start_mode = earliest
    schema = {
      fields {
           id = "int"
           name = "string"
           description = "string"
           weight = "string"
      }
    }
    format = debezium_json
  }

}

transform {
}

sink {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "consume-binlog"
    format = debezium_json
  }
}
```

