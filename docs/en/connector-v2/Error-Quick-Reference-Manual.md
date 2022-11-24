# Error Quick Reference Manual

This document records some common error codes and corresponding solutions of SeaTunnel, aiming to quickly solve the
problems encountered by users.

## SeaTunnel API Error Codes

| code   | description                        | solution                                                                                                                                                                                       |
|--------|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| API-01 | Configuration item validate failed | When users encounter this error code, it is usually due to a problem with the connector parameters configured by the user, please check the connector documentation and correct the parameters |
| API-02 | Option item validate failed        | -                                                                                                                                                                                              |
| API-03 | Catalog initialize failed          | When users encounter this error code, it is usually because the connector initialization catalog failed, please check the connector connector options whether are correct                      |
| API-04 | Database not existed               | When users encounter this error code, it is usually because the database that you want to access is not existed, please double check the database exists                                       |
| API-05 | Table not existed                  | When users encounter this error code, it is usually because the table that you want to access is not existed, please double check the table exists                                             |
| API-06 | Factory initialize failed          | When users encounter this error code, it is usually because there is a problem with the jar package dependency, please check whether your local SeaTunnel installation package is complete     |

## SeaTunnel Common Error Codes

| code      | description                                                            | solution                                                                                                                                                                                           |
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

## Assert Connector Error Codes

| code      | description          | solution                                                                                  |
|-----------|----------------------|-------------------------------------------------------------------------------------------|
| ASSERT-01 | Rule validate failed | When users encounter this error code, it means that upstream data does not meet the rules |

## Cassandra Connector Error Codes

| code         | description                                     | solution                                                                                                                                                              |
|--------------|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CASSANDRA-01 | Field is not existed in target table            | When users encounter this error code, it means that the fields of upstream data don't meet with target cassandra table, please check target cassandra table structure |
| CASSANDRA-02 | Add batch SeaTunnelRow data into a batch failed | When users encounter this error code, it means that cassandra has some problems, please check it whether is work                                                      |
| CASSANDRA-03 | Close cql session of cassandra failed           | When users encounter this error code, it means that cassandra has some problems, please check it whether is work                                                      |
| CASSANDRA-04 | No data in source table                         | When users encounter this error code, it means that source cassandra table has no data, please check it                                                               |
| CASSANDRA-05 | Parse ip address from string field field        | When users encounter this error code, it means that upstream data does not match ip address format, please check it                                                   |

## Slack Connector Error Codes

| code      | description                                 | solution                                                                                                           |
|-----------|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| SLACK-01  | Conversation can not be founded in channels | When users encounter this error code, it means that the channel is not existed in slack workspace, please check it |
| SLACK-02  | Write to slack channel failed               | When users encounter this error code, it means that slack has some problems, please check it whether is work       |

## Socket Connector Error Codes

| code      | description                                              | solution                                                                                                                       |
|-----------|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| SOCKET-01 | Cannot connect to socket server                          | When the user encounters this error code, it means that the connection for address may not match, please check                 |
| SOCKET-02 | Failed to send message to socket server                  | When the user encounters this error code, it means that there is a problem sending data and retry is not enabled, please check |
| SOCKET-03 | Unable to write; interrupted while doing another attempt | When the user encounters this error code, it means that the data writing is interrupted abnormally, please check               |

## Elasticsearch Connector Error Codes

| code              | description                                   | solution                                                                                                                                 |
|-------------------|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| ELASTICSEARCH-01  | Bulk es response error                        | When the user encounters this error code, it means that the connection address may not match or the connection was aborted, please check |
| ELASTICSEARCH-02  | Get elasticsearch version failed              | When the user encounters this error code, it means that the connection address may not match or the connection was aborted, please check |
| ELASTICSEARCH-03  | Fail to scroll request                        | When the user encounters this error code, it means that the connection address may not match or the connection was aborted, please check |
| ELASTICSEARCH-04  | Get elasticsearch document index count failed | When the user encounters this error code, it means that the connection address may not match or the connection was aborted, please check |
