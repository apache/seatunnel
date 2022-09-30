# Kafka

> Kafka sink connector

## Description

Write Rows to a Kafka topic.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we will use 2pc to guarantee the message is sent to kafka exactly once.

- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name               | type                   | required | default value |
| ------------------ | ---------------------- | -------- | ------------- |
| topic              | string                 | yes      | -             |
| bootstrap.servers  | string                 | yes      | -             |
| kafka.*            | kafka producer config  | no       | -             |
| semantic           | string                 | no       | NON           |
| partition          | int                    | no       | -             |
| transaction_prefix | string                 | no       | -             |
| common-options     |                        | no       | -             |

### topic [string]

Kafka Topic.

### bootstrap.servers [string]

Kafka Brokers List.

### kafka.* [kafka producer config]

In addition to the above parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).

The way to specify the parameter is to add the prefix `kafka.` to the original parameter name. For example, the way to specify `request.timeout.ms` is: `kafka.request.timeout.ms = 60000` . If these non-essential parameters are not specified, they will use the default values given in the official Kafka documentation.

### semantic [string]

Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.

In EXACTLY_ONCE, producer will write all messages in a Kafka transaction that will be committed to Kafka on a checkpoint.

In AT_LEAST_ONCE, producer will wait for all outstanding messages in the Kafka buffers to be acknowledged by the Kafka producer on a checkpoint.

NON does not provide any guarantees: messages may be lost in case of issues on the Kafka broker and messages may be duplicated.

### partition [int]

We can specify the partition, all messages will be sent to this partition.

### transaction_prefix [string]

If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Kafka transaction.
Kafka distinguishes different transactions by different transactionId. This parameter is prefix of  kafka  transactionId, make sure different job use different prefix.

### common options 

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Examples

#### Exactly_Once

```hocon
sink {

  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
      partition = 3
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
  }
  
}
```

#### Custom Partitioner

We should implement the `org.apache.kafka.clients.producer.Partitioner` interface, for example:

```java
//Determine the partition to send based on the content of the message
public class CustomPartitioner implements Partitioner {
    List<String> assignPartitions =  Arrays.asList("shoe", "clothing");
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //Get the total number of partitions
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        int assignPartitionsSize = assignPartitions.size();
        String message = new String(valueBytes);
        for (int i = 0; i < assignPartitionsSize; i++) {
            if (message.contains(assignPartitions.get(i))) {
                return i;
            }
        }
        //Choose one of the remaining partitions according to the hashcode.
        return ((message.hashCode() & Integer.MAX_VALUE) % (numPartitions - assignPartitionsSize)) + assignPartitionsSize;
    }
}
```

We also need to configure the full class name.

```hocon
sink {

  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
	  kafka.partitioner.class = "org.apache.seatunnel.connectors.seatunnel.kafka.sink.CustomPartitioner"
  }
  
}
```

