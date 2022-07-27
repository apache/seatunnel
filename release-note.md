# 2.1.3 Release Note

## [Feature & Improvement]

[Connector][Flink][Fake] Supported BigInteger Type (#2118)

[Connector][Spark][TiDB] Refactored config parameters (#1983)

[Connector][Flink]add AssertSink connector (#2022) 

[Connector][Spark][ClickHouse]Support Rsync to transfer clickhouse data file (#2074)

[Connector & e2e][Flink] add IT for Assert Sink in e2e module (#2036)

[Transform][Spark] data quality for null data rate (#1978)

[Transform][Spark] Add a module to set default value for null field #1958

[Chore]a more understandable code,and code warning will disappear #2005

[Spark] Use higher version of the libthrift dependency (#1994)

[Core][Starter] Change jar connector load logic (#2193)

[Core]Add plugin discovery module (#1881)

## [BUG]

[Connector][Hudi] Source loads the data twice

[Connector][Doris]Fix the bug Unrecognized field "TwoPhaseCommit" after doris 0.15 (#2054) 

[Connector][Jdbc]Fix the data output exception when accessing Hive using Spark JDBC #2085

[Connector][Jdbc]Fix JDBC data loss occurs when partition_column (partition mode) is set #2033

[Connector][Kafka]KafkaTableStream schema json parse #2168

[seatunnel-core] Failed to get APP_DIR path bug fixed (#2165)

[seatunnel-api-flink] Connectors dependencies repeat additions (#2207)

[seatunnel-core] Failed to get APP_DIR path bug fixed (#2165)

[seatunnel-core-flink] Updated FlinkRunMode enum to get the proper help message for run modes. (#2008)

[seatunnel-core-flink]fix same source and sink registerplugin librarycache error (#2015)

[Command]fix commandArgs -t(--check) conflict with flink deployment target (#2174)

[Core][Jackson]fix jackson type convert error (#2031)

[Core][Starter] When use cluster mode, but starter app root dir also should same as client mode. (#2141)

## Docs

source socket connector docs update (#1995)

Add uuid, udf, replace transform to doc (#2016) 

Update Flink engine version requirements (#2220)

Add Flink SQL module to website. (#2021) 

[kubernetes] update seatunnel doc on kubernetes (#2035)

## Dependency upgrade

Upgrade common-collecions4 to 4.4

Upgrade common-codec to 1.13
