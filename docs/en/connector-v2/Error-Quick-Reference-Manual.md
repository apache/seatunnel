# Error Quick Reference Manual

This document records some common error codes and corresponding solutions of SeaTunnel, aiming to quickly solve the
problems encountered by users.

## SeaTunnel API Error Codes

|  code  |            description             |                                                                                            solution                                                                                            |
|--------|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| API-01 | Configuration item validate failed | When users encounter this error code, it is usually due to a problem with the connector parameters configured by the user, please check the connector documentation and correct the parameters |
| API-02 | Option item validate failed        | -                                                                                                                                                                                              |
| API-03 | Catalog initialize failed          | When users encounter this error code, it is usually because the connector initialization catalog failed, please check the connector connector options whether are correct                      |
| API-04 | Database not existed               | When users encounter this error code, it is usually because the database that you want to access is not existed, please double check the database exists                                       |
| API-05 | Table not existed                  | When users encounter this error code, it is usually because the table that you want to access is not existed, please double check the table exists                                             |
| API-06 | Factory initialize failed          | When users encounter this error code, it is usually because there is a problem with the jar package dependency, please check whether your local SeaTunnel installation package is complete     |
| API-07 | Database already existed           | When users encounter this error code, it means that the database you want to create has already existed, please delete database and try again                                                  |
| API-08 | Table already existed              | When users encounter this error code, it means that the table you want to create has already existed, please delete table and try again                                                        |

## SeaTunnel Common Error Codes

|   code    |                              description                               |                                                                                              solution                                                                                              |
|-----------|------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| COMMON-01 | File operation failed, such as (read,list,write,move,copy,sync) etc... | When users encounter this error code, it is usually there are some problems in the file operation, please check if the file is OK                                                                  |
| COMMON-02 | Json covert/parse operation failed                                     | When users encounter this error code, it is usually there are some problems about json converting or parsing, please check if the json format is correct                                           |
| COMMON-03 | Reflect class operation failed                                         | When users encounter this error code, it is usually there are some problems on class reflect operation, please check the jar dependency whether exists in classpath                                |
| COMMON-04 | Serialize class operation failed                                       | When users encounter this error code, it is usually there are some problems on class serialize operation, please check java environment                                                            |
| COMMON-05 | Unsupported operation                                                  | When users encounter this error code, users may trigger an unsupported operation such as enabled some unsupported features                                                                         |
| COMMON-06 | Illegal argument                                                       | When users encounter this error code, it maybe user-configured parameters are not legal, please correct it according to the tips                                                                   |
| COMMON-07 | Unsupported data type                                                  | When users encounter this error code, it maybe connectors don't support this data type                                                                                                             |
| COMMON-08 | Sql operation failed, such as (execute,addBatch,close) etc...          | When users encounter this error code, it is usually there are some problems on sql execute process, please check the sql whether correct                                                           |
| COMMON-09 | Get table schema from upstream data failed                             | When users encounter this error code, it maybe SeaTunnel try to get schema information from connector source data failed, please check your configuration whether correct and connector is work    |
| COMMON-10 | Flush data operation that in sink connector failed                     | When users encounter this error code, it maybe SeaTunnel try to flush batch data to sink connector field, please check your configuration whether correct and connector is work                    |
| COMMON-11 | Sink writer operation failed, such as (open, close) etc...             | When users encounter this error code, it maybe some operation of writer such as Parquet,Orc,IceBerg failed, you need to check if the corresponding file or resource has read and write permissions |
| COMMON-12 | Source reader operation failed, such as (open, close) etc...           | When users encounter this error code, it maybe some operation of reader such as Parquet,Orc,IceBerg failed, you need to check if the corresponding file or resource has read and write permissions |
| COMMON-13 | Http operation failed, such as (open, close, response) etc...          | When users encounter this error code, it maybe some http requests failed, please check your network environment                                                                                    |
| COMMON-14 | Kerberos authorized failed                                             | When users encounter this error code, it maybe some The Kerberos authorized is misconfigured                                                                                                       |
| COMMON-15 | Class load operation failed                                            | When users encounter this error code, it maybe some The corresponding jar does not exist, or the type is not supported                                                                             |

## Assert Connector Error Codes

|   code    |     description      |                                         solution                                          |
|-----------|----------------------|-------------------------------------------------------------------------------------------|
| ASSERT-01 | Rule validate failed | When users encounter this error code, it means that upstream data does not meet the rules |

## Cassandra Connector Error Codes

|     code     |                   description                   |                                                                               solution                                                                                |
|--------------|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CASSANDRA-01 | Field is not existed in target table            | When users encounter this error code, it means that the fields of upstream data don't meet with target cassandra table, please check target cassandra table structure |
| CASSANDRA-02 | Add batch SeaTunnelRow data into a batch failed | When users encounter this error code, it means that cassandra has some problems, please check it whether is work                                                      |
| CASSANDRA-03 | Close cql session of cassandra failed           | When users encounter this error code, it means that cassandra has some problems, please check it whether is work                                                      |
| CASSANDRA-04 | No data in source table                         | When users encounter this error code, it means that source cassandra table has no data, please check it                                                               |
| CASSANDRA-05 | Parse ip address from string failed             | When users encounter this error code, it means that upstream data does not match ip address format, please check it                                                   |

## Slack Connector Error Codes

|   code   |                 description                 |                                                      solution                                                      |
|----------|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| SLACK-01 | Conversation can not be founded in channels | When users encounter this error code, it means that the channel is not existed in slack workspace, please check it |
| SLACK-02 | Write to slack channel failed               | When users encounter this error code, it means that slack has some problems, please check it whether is work       |

## MyHours Connector Error Codes

|    code    |       description        |                                                         solution                                                         |
|------------|--------------------------|--------------------------------------------------------------------------------------------------------------------------|
| MYHOURS-01 | Get myhours token failed | When users encounter this error code, it means that login to the MyHours Failed, please check your network and try again |

## Rabbitmq Connector Error Codes

|    code     |                          description                          |                                                    solution                                                     |
|-------------|---------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| RABBITMQ-01 | handle queue consumer shutdown signal failed                  | When users encounter this error code, it means that job has some problems, please check it whether is work well |
| RABBITMQ-02 | create rabbitmq client failed                                 | When users encounter this error code, it means that rabbitmq has some problems, please check it whether is work |
| RABBITMQ-03 | close connection failed                                       | When users encounter this error code, it means that rabbitmq has some problems, please check it whether is work |
| RABBITMQ-04 | send messages failed                                          | When users encounter this error code, it means that rabbitmq has some problems, please check it whether is work |
| RABBITMQ-05 | messages could not be acknowledged during checkpoint creation | When users encounter this error code, it means that job has some problems, please check it whether is work well |
| RABBITMQ-06 | messages could not be acknowledged with basicReject           | When users encounter this error code, it means that job has some problems, please check it whether is work well |
| RABBITMQ-07 | parse uri failed                                              | When users encounter this error code, it means that rabbitmq connect uri incorrect, please check it             |
| RABBITMQ-08 | initialize ssl context failed                                 | When users encounter this error code, it means that rabbitmq has some problems, please check it whether is work |
| RABBITMQ-09 | setup ssl factory failed                                      | When users encounter this error code, it means that rabbitmq has some problems, please check it whether is work |

## Socket Connector Error Codes

|   code    |                       description                        |                                                            solution                                                            |
|-----------|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| SOCKET-01 | Cannot connect to socket server                          | When the user encounters this error code, it means that the connection address may not match, please check                     |
| SOCKET-02 | Failed to send message to socket server                  | When the user encounters this error code, it means that there is a problem sending data and retry is not enabled, please check |
| SOCKET-03 | Unable to write; interrupted while doing another attempt | When the user encounters this error code, it means that the data writing is interrupted abnormally, please check               |

## TableStore Connector Error Codes

|     code      |            description            |                                                              solution                                                               |
|---------------|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| TABLESTORE-01 | Failed to send these rows of data | When users encounter this error code, it means that failed to write these rows of data, please check the rows that failed to import |

## Hive Connector Error Codes

|  code   |                          description                          |                                                           solution                                                            |
|---------|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| HIVE-01 | Get name node host from table location failed                 | When users encounter this error code, it means that the metastore inforamtion has some problems, please check it              |
| HIVE-02 | Initialize hive metastore client failed                       | When users encounter this error code, it means that connect to hive metastore service failed, please check it whether is work |
| HIVE-03 | Get hive table information from hive metastore service failed | When users encounter this error code, it means that hive metastore service has some problems, please check it whether is work |

## Elasticsearch Connector Error Codes

|       code       |                  description                  |                                                            solution                                                            |
|------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| ELASTICSEARCH-01 | Bulk es response error                        | When the user encounters this error code, it means that the connection was aborted, please check it whether is work            |
| ELASTICSEARCH-02 | Get elasticsearch version failed              | When the user encounters this error code, it means that the connection was aborted, please check it whether is work            |
| ELASTICSEARCH-03 | Fail to scroll request                        | When the user encounters this error code, it means that the connection was aborted, please check it whether is work            |
| ELASTICSEARCH-04 | Get elasticsearch document index count failed | When the user encounters this error code, it means that the es index may not wrong or the connection was aborted, please check |

## Kafka Connector Error Codes

|   code   |                                       description                                       |                                                             solution                                                              |
|----------|-----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| KAFKA-01 | Incompatible KafkaProducer version                                                      | When users encounter this error code, it means that KafkaProducer version is incompatible, please check it                        |
| KAFKA-02 | Get transactionManager in KafkaProducer exception                                       | When users encounter this error code, it means that can not get transactionManager in KafkaProducer, please check it              |
| KAFKA-03 | Add the split checkpoint state to reader failed                                         | When users encounter this error code, it means that add the split checkpoint state to reader failed, please retry it              |
| KAFKA-04 | Add a split back to the split enumerator,it will only happen when a SourceReader failed | When users encounter this error code, it means that add a split back to the split enumerator failed, please check it              |
| KAFKA-05 | Error occurred when the kafka consumer thread was running                               | When users encounter this error code, it means that an error occurred when the kafka consumer thread was running, please check it |
| KAFKA-06 | Kafka failed to consume data                                                            | When users encounter this error code, it means that Kafka failed to consume data, please check config and retry it                |
| KAFKA-07 | Kafka failed to close consumer                                                          | When users encounter this error code, it means that Kafka failed to close consumer                                                |

## InfluxDB Connector Error Codes

|    code     |                           description                            |                                                  solution                                                   |
|-------------|------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| INFLUXDB-01 | Connect influxdb failed, due to influxdb version info is unknown | When the user encounters this error code, it indicates that the connection to influxdb failed. Please check |
| INFLUXDB-02 | Get column index of query result exception                       | When the user encounters this error code, it indicates that obtaining the column index failed. Please check |

## Kudu Connector Error Codes

|  code   |                       description                        |                                                                                            solution                                                                                            |
|---------|----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| KUDU-01 | Get the Kuduscan object for each splice failed           | When users encounter this error code, it is usually there are some problems with getting the KuduScan Object for each splice, please check your configuration whether correct and Kudu is work |
| KUDU-02 | Close Kudu client failed                                 | When users encounter this error code, it is usually there are some problems with closing the Kudu client, please check the Kudu is work                                                        |   |
| KUDU-03 | Value type does not match column type                    | When users encounter this error code, it is usually there are some problems on matching the Type between value type and colum type, please check if the data type is supported                 |
| KUDU-04 | Upsert data to Kudu failed                               | When users encounter this error code, it means that Kudu has some problems, please check it whether is work                                                                                    |
| KUDU-05 | Insert data to Kudu failed                               | When users encounter this error code, it means that Kudu has some problems, please check it whether is work                                                                                    |
| KUDU-06 | Initialize the Kudu client failed                        | When users encounter this error code, it is usually there are some problems with initializing the Kudu client, please check your configuration whether correct and connector is work           |
| KUDU-07 | Generate Kudu Parameters in the preparation phase failed | When users encounter this error code, it means that there are some problems on Kudu parameters generation, please check your configuration                                                     |

## IotDB Connector Error Codes

|   code   |          description           |                                                  solution                                                  |
|----------|--------------------------------|------------------------------------------------------------------------------------------------------------|
| IOTDB-01 | Close IoTDB session failed     | When the user encounters this error code, it indicates that closing the session failed. Please check       |
| IOTDB-02 | Initialize IoTDB client failed | When the user encounters this error code, it indicates that the client initialization failed. Please check |
| IOTDB-03 | Close IoTDB client failed      | When the user encounters this error code, it indicates that closing the client failed. Please check        |

## File Connector Error Codes

|  code   |         description         |                                                                             solution                                                                             |
|---------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| FILE-01 | File type is invalid        | When users encounter this error code, it means that the this file is not the format that user assigned, please check it                                          |
| FILE-02 | Data deserialization failed | When users encounter this error code, it means that data from files not satisfied the schema that user assigned, please check data from files whether is correct |
| FILE-03 | Get file list failed        | When users encounter this error code, it means that connector try to traverse the path and get file list failed, please check file system whether is work        |
| FILE-04 | File list is empty          | When users encounter this error code, it means that the path user want to sync is empty, please check file path                                                  |

## Doris Connector Error Codes

|   code   |     description     |                                                             solution                                                              |
|----------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| Doris-01 | stream load error.  | When users encounter this error code, it means that stream load to Doris failed, please check data from files whether is correct. |
| Doris-02 | commit error.       | When users encounter this error code, it means that commit to Doris failed, please check network.                                 |
| Doris-03 | rest service error. | When users encounter this error code, it means that rest service failed, please check network and config.                         |

## SelectDB Cloud Connector Error Codes

|    code     |         description         |                                                                 solution                                                                  |
|-------------|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| SelectDB-01 | stage load file error       | When users encounter this error code, it means that stage load file to SelectDB Cloud failed, please check the configuration and network. |
| SelectDB-02 | commit copy into sql failed | When users encounter this error code, it means that commit copy into sql to SelectDB Cloud failed, please check the configuration.        |

## Clickhouse Connector Error Codes

|     code      |                                description                                |                                                                                solution                                                                                 |
|---------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CLICKHOUSE-01 | Field is not existed in target table                                      | When users encounter this error code, it means that the fields of upstream data don't meet with target clickhouse table, please check target clickhouse table structure |
| CLICKHOUSE-02 | Can’t find password of shard node                                         | When users encounter this error code, it means that no password is configured for each node, please check                                                               |
| CLICKHOUSE-03 | Can’t delete directory                                                    | When users encounter this error code, it means that the directory does not exist or does not have permission, please check                                              |
| CLICKHOUSE-04 | Ssh operation failed, such as (login,connect,authentication,close) etc... | When users encounter this error code, it means that the ssh request failed, please check your network environment                                                       |
| CLICKHOUSE-05 | Get cluster list from clickhouse failed                                   | When users encounter this error code, it means that the clickhouse cluster is not configured correctly, please check                                                    |
| CLICKHOUSE-06 | Shard key not found in table                                              | When users encounter this error code, it means that the shard key of the distributed table is not configured, please check                                              |

## Jdbc Connector Error Codes

|  code   |                          description                           |                                                                                                  solution                                                                                                   |
|---------|----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| JDBC-01 | Fail to create driver of class                                 | When users encounter this error code, it means that driver package may not be added. Check whether the driver exists                                                                                        |
| JDBC-02 | No suitable driver found                                       | When users encounter this error code, it means that no password is configured for each node, please check                                                                                                   |
| JDBC-03 | Xa operation failed, such as (commit, rollback) etc..          | When users encounter this error code, it means that if a distributed sql transaction fails, check the transaction execution of the corresponding database to determine the cause of the transaction failure |
| JDBC-04 | Connector database failed                                      | When users encounter this error code, it means that database connection failure, check whether the url is correct or whether the corresponding service is normal                                            |
| JDBC-05 | transaction operation failed, such as (commit, rollback) etc.. | When users encounter this error code, it means that if a sql transaction fails, check the transaction execution of the corresponding database to determine the cause of the transaction failure             |
| JDBC-06 | No suitable dialect factory found                              | When users encounter this error code, it means that may be an unsupported dialect type                                                                                                                      |

## Pulsar Connector Error Codes

|   code    |                   description                    |                                                       solution                                                        |
|-----------|--------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| PULSAR-01 | Open pulsar admin failed                         | When users encounter this error code, it means that open pulsar admin failed, please check it                         |
| PULSAR-02 | Open pulsar client failed                        | When users encounter this error code, it means that open pulsar client failed, please check it                        |
| PULSAR-03 | Pulsar authentication failed                     | When users encounter this error code, it means that Pulsar Authentication failed, please check it                     |
| PULSAR-04 | Subscribe topic from pulsar failed               | When users encounter this error code, it means that Subscribe topic from pulsar failed, please check it               |
| PULSAR-05 | Get last cursor of pulsar topic failed           | When users encounter this error code, it means that get last cursor of pulsar topic failed, please check it           |
| PULSAR-06 | Get partition information of pulsar topic failed | When users encounter this error code, it means that Get partition information of pulsar topic failed, please check it |
| PULSAR-07 | Pulsar consumer acknowledgeCumulative failed     | When users encounter this error code, it means that Pulsar consumer acknowledgeCumulative failed                      |

## StarRocks Connector Error Codes

|     code     |                description                |                                                                 solution                                                                 |
|--------------|-------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| STARROCKS-01 | Flush batch data to sink connector failed | When users encounter this error code, it means that flush batch data to sink connector failed, please check it                           |
| STARROCKS-02 | Writing records to StarRocks failed       | When users encounter this error code, it means that writing records to StarRocks failed, please check data from files whether is correct |
| STARROCKS-03 | Close StarRocks BE reader failed.         | it means that StarRocks has some problems, please check it whether is work                                                               |
| STARROCKS-04 | Create StarRocks BE reader failed.        | it means that StarRocks has some problems, please check it whether is work                                                               |
| STARROCKS-05 | Scan data from StarRocks BE failed.       | When users encounter this error code, it means that scan data from StarRocks failed, please check it                                     |
| STARROCKS-06 | Request query Plan failed.                | When users encounter this error code, it means that scan data from StarRocks failed, please check it                                     |
| STARROCKS-07 | Read Arrow data failed.                   | When users encounter this error code, it means that that job has some problems, please check it whether is work well                     |

## DingTalk Connector Error Codes

|    code     |               description               |                                                       solution                                                       |
|-------------|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| DINGTALK-01 | Send response to DinkTalk server failed | When users encounter this error code, it means that send response message to DinkTalk server failed, please check it |
| DINGTALK-02 | Get sign from DinkTalk server failed    | When users encounter this error code, it means that get signature from DinkTalk server failed , please check it      |

## Iceberg Connector Error Codes

|    code    |          description           |                                                 solution                                                 |
|------------|--------------------------------|----------------------------------------------------------------------------------------------------------|
| ICEBERG-01 | File Scan Split failed         | When users encounter this error code, it means that the file scanning and splitting failed. Please check |
| ICEBERG-02 | Invalid starting record offset | When users encounter this error code, it means that the starting record offset is invalid. Please check  |

## Email Connector Error Codes

|   code   |    description    |                                                                              solution                                                                               |
|----------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| EMAIL-01 | Send email failed | When users encounter this error code, it means that send email to target server failed, please adjust the network environment according to the abnormal information |

## S3Redshift Connector Error Codes

|     code      |        description        |                                                                                                   solution                                                                                                   |
|---------------|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| S3RedShift-01 | Aggregate committer error | S3Redshift Sink Connector will write data to s3 and then move file to the target s3 path. And then use `Copy` action copy the data to Redshift. Please check the error log and find out the specific reason. |

## Google Firestore Connector Error Codes

|     code     |          description          |                                                                     solution                                                                      |
|--------------|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| FIRESTORE-01 | Close Firestore client failed | When users encounter this error code, it is usually there are some problems with closing the Firestore client, please check the Firestore is work |

## FilterFieldTransform Error Codes

|           code            |      description       |        solution         |
|---------------------------|------------------------|-------------------------|
| FILTER_FIELD_TRANSFORM-01 | filter field not found | filter field not found. |

## RocketMq Connector Error Codes

|    code     |                                           description                                           |                                                           solution                                                            |
|-------------|-------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| ROCKETMQ-01 | Add a split back to the split enumerator failed, it will only happen when a SourceReader failed | When users encounter this error code, it means that add a split back to the split enumerator failed, please check it.         |
| ROCKETMQ-02 | Add the split checkpoint state to reader failed                                                 | When users encounter this error code, it means that add the split checkpoint state to reader failed, please check it.         |
| ROCKETMQ-03 | Rocketmq failed to consume data                                                                 | When users encounter this error code, it means that rocketmq failed to consume data, please check it., please check it.       |
| ROCKETMQ-04 | Error occurred when the rocketmq consumer thread was running                                    | When the user encounters this error code, it means that an error occurred while running the Rocketmq consumer thread          |
| ROCKETMQ-05 | Rocketmq producer failed to send message                                                        | When users encounter this error code, it means that Rocketmq producer failed to send message, please check it.                |
| ROCKETMQ-06 | Rocketmq producer failed to start                                                               | When users encounter this error code, it means that Rocketmq producer failed to start, please check it.                       |
| ROCKETMQ-07 | Rocketmq consumer failed to start                                                               | When users encounter this error code, it means that Rocketmq consumer failed to start, please check it.                       |
| ROCKETMQ-08 | Unsupported start mode                                                                          | When users encounter this error code, it means that the configured start mode is not supported, please check it.              |
| ROCKETMQ-09 | Failed to get the offsets of the current consumer group                                         | When users encounter this error code, it means that failed to get the offsets of the current consumer group, please check it. |
| ROCKETMQ-10 | Failed to search offset through timestamp                                                       | When users encounter this error code, it means that failed to search offset through timestamp, please check it.               |
| ROCKETMQ-11 | Failed to get topic min and max topic                                                           | When users encounter this error code, it means that failed to get topic min and max topic, please check it.                   |

