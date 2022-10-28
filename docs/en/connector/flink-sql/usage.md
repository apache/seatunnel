# How to use flink sql module

> Tutorial of flink sql module

## Usage

### 1. Command Entrypoint

```bash
bin/start-seatunnel-sql.sh
```

### 2. seatunnel config

Change the file flink.sql.conf.template in the config/ directory to flink.sql.conf

```bash
mv flink.sql.conf.template flink.sql.conf
```

Prepare a seatunnel config file with the following content:

```sql
SET table.dml-sync = true;

CREATE TABLE events (
  f_type INT,
  f_uid INT,
  ts AS localtimestamp,
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'datagen',
  'rows-per-second'='5',
  'fields.f_type.min'='1',
  'fields.f_type.max'='5',
  'fields.f_uid.min'='1',
  'fields.f_uid.max'='1000'
);

CREATE TABLE print_table (
  type INT,
  uid INT,
  lstmt TIMESTAMP
) WITH (
  'connector' = 'print',
  'sink.parallelism' = '1'
);

INSERT INTO print_table SELECT * FROM events where f_type = 1;
```

### 3. run job

#### Standalone Cluster

```bash
bin/start-seatunnel-sql.sh --config config/flink.sql.conf

# -p 2 specifies that the parallelism of flink job is 2. You can also specify more parameters, use flink run -h to view
bin/start-seatunnel-flink.sh \
-p 2 \
--config config/flink.sql.conf
```

#### Yarn Cluster

```bash
bin/start-seatunnel-sql.sh -m yarn-cluster --config config/flink.sql.conf

bin/start-seatunnel-sql.sh -t yarn-per-job --config config/flink.sql.conf

# -p 2 specifies that the parallelism of flink job is 2. You can also specify more parameters, use flink run -h to view
bin/start-seatunnel-flink.sh \
-p 2 \
-m yarn-cluster \
--config config/flink.sql.conf
```

#### Other Options

* `-p 2` specifies that the job parallelism is `2`

```bash
bin/start-seatunnel-sql.sh -p 2 --config config/flink.sql.conf
```

## Example

1. How to implement flink sql interval join with seatunnel flink-sql module

intervaljoin.sql.conf

```hocon
CREATE TABLE basic (
  `id` BIGINT,
  `name` STRING,
   `ts`  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'basic',
  'properties.bootstrap.servers' = 'XX.XX.XX.XX:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE infos (
  `id` BIGINT,
  `age` BIGINT,
   `ts`  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'info',
  'properties.bootstrap.servers' = 'XX.XX.XX.XX:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE stream2_join_result (
  id BIGINT , 
  name STRING,
  age BIGINT,
  ts1 STRING , 
  ts2 STRING,
  PRIMARY KEY(id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://XX.XX.XX.XX:3306/testDB',
  'username' = 'root',
  'password' = 'taia@2021',
  'table-name' = 'stream2_join_result'
);

insert into  stream2_join_result select basic.id, basic.name, infos.age,basic.ts,infos.ts 
from basic join infos on (basic.id = infos.id) where  TO_TIMESTAMP(basic.ts,'yyyy-MM-dd HH:mm:ss') 
BETWEEN   TO_TIMESTAMP(infos.ts,'yyyy-MM-dd HH:mm:ss')  - INTERVAL '10' SECOND AND  TO_TIMESTAMP(infos.ts,'yyyy-MM-dd HH:mm:ss') + INTERVAL '10' SECOND;
```

```bash
bin/start-seatunnel-sql.sh -m yarn-cluster --config config/intervaljoin.sql.conf
```

2. How to implement flink sql dim join (using mysql) with seatunnel flink-sql module

dimjoin.sql.conf

```hocon
CREATE TABLE code_set_street (
  area_code STRING,
  area_name STRING,
  town_code STRING ,
  town_name STRING ,
  PRIMARY KEY(town_code) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://XX.XX.XX.XX:3306/testDB',
  'username' = 'root',
  'password' = '2021',
  'table-name' = 'code_set_street',
  'lookup.cache.max-rows' = '5000' ,
  'lookup.cache.ttl' = '5min'
);

CREATE TABLE people (
  `id` STRING,
  `name` STRING,
  `ts`  TimeStamp(3) ,
  proctime AS PROCTIME() 
) WITH (
  'connector' = 'kafka',
  'topic' = 'people',
  'properties.bootstrap.servers' = 'XX.XX.XX.XX:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE mysql_dim_join_result (
  id STRING , 
  name STRING,
  area_name STRING,
  town_code STRING , 
  town_name STRING,
  ts TimeStamp ,
  PRIMARY KEY(id,town_code) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://XX.XX.XX.XX:3306/testDB',
  'username' = 'root',
  'password' = '2021',
  'table-name' = 'mysql_dim_join_result'
);

insert into mysql_dim_join_result
select people.id , people.name ,code_set_street.area_name ,code_set_street.town_code, code_set_street.town_name , people.ts  
from people inner join code_set_street FOR SYSTEM_TIME AS OF  people.proctime  
on (people.id = code_set_street.town_code);
```

```bash
bin/start-seatunnel-sql.sh -m yarn-cluster --config config/dimjoin.sql.conf
```

3. How to implement flink SQL cdc dim join (using mysql-cdc) with seatunnel flink-sql module

##### First, we need to create a table in mysql database

```
CREATE TABLE `dim_cdc_join_result` (
    `id` varchar(255) NOT NULL,
    `name` varchar(255) DEFAULT NULL,
    `area_name` varchar(255) NOT NULL,
    `town_code` varchar(255) NOT NULL,
    `town_name` varchar(255) DEFAULT NULL,
    `ts` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`id`,`town_code`,`ts`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;
```

cdcjoin.sql.conf

```hocon
CREATE TABLE code_set_street_cdc (
  area_code STRING,
  area_name STRING,
  town_code STRING ,
  town_name STRING ,
  PRIMARY KEY(town_code) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'XX.XX.XX.XX',
  'port' = '3306',
  'username' = 'root',
  'password' = '2021',
  'database-name' = 'flink',
  'table-name' = 'code_set_street'
);
     
CREATE TABLE people (
  `id` STRING,
  `name` STRING,
  `ts`  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'people',
  'properties.bootstrap.servers' = 'XX.XX.XX.XX:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

# create mysql sink table in flink
CREATE TABLE dim_cdc_join_result (
  id STRING , 
  name STRING,
  area_name STRING,
  town_code STRING , 
  town_name STRING,
  ts STRING ,
  PRIMARY KEY(id,town_code) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://XX.XX.XX.XX:3306/flink',
  'username' = 'root',
  'password' = '2021',
  'table-name' = 'dim_cdc_join_result'
);
 
insert into dim_cdc_join_result
select a.id , a.name ,b.area_name ,b.town_code, b.town_name , a.ts  
from people a inner join code_set_street_cdc b  on (a.id = b.town_code);
```

```bash
bin/start-seatunnel-sql.sh -m yarn-cluster --config config/cdcjoin.sql.conf
```