# Sink plugin: Doirs

### Description:
Use Spark Batch Engine ETL Data to Doris.

### Options
| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| host | string | yes | - | Spark |
| database | string | yes | - | Spark |
| tableName	 | string | yes | - | Spark |
| user	 | string | yes | - | Spark |
| password	 | string | yes | - | Spark |
| bulk_size	 | int | yes | - | Spark |
| doris.*	 | string | no | - | Spark |

##### host [string]
Doris FE address:8030

##### database [string]
Doris target database name
##### tableName [string]
Doris target table name
##### user [string]
Doris user name
##### password [string]
Doris user's password
##### bulk_size [string]
Doris number of submissions per batch
##### doris. [string]
Doris stream_load properties,you can use 'doris.' prefix + stream_load properties

[More Doris stream_load Configurations](https://doris.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)

### Examples

```
doris {
            host="0.0.0.0:8030"
            database="test"
            tableName="user"
            user="doris"
            password="doris"
            bulk_size=10000
            doris.column_separator="\t"
            doris.columns="id,user_name,user_name_cn,create_time,last_login_time"
      
      }
```