# Sink plugin: Doirs [Spark]

### Description:
Use Spark Batch Engine ETL Data to Doris.

### Options
| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Spark |
| database | string | yes | - | Spark |
| table	 | string | yes | - | Spark |
| user	 | string | yes | - | Spark |
| password	 | string | yes | - | Spark |
| batch_size	 | int | yes | 100 | Spark |
| doris.*	 | string | no | - | Spark |

##### fenodes [string]
Doris FE address:8030

##### database [string]
Doris target database name
##### table [string]
Doris target table name
##### user [string]
Doris user name
##### password [string]
Doris user's password
##### batch_size [string]
Doris number of submissions per batch
##### doris. [string]
Doris stream_load properties,you can use 'doris.' prefix + stream_load properties
[More Doris stream_load Configurations](https://doris.apache.org/administrator-guide/load-data/stream-load-manual.html)

### Examples

```
Doris {
            fenodes="0.0.0.0:8030"
            database="test"
            table="user"
            user="doris"
            password="doris"
            batch_size=10000
            doris.column_separator="\t"
            doris.columns="id,user_name,user_name_cn,create_time,last_login_time"
      
      }
```
