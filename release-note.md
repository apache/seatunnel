# 2.1.1 Release

[Feature]
- Support json format config file
- Jdbc connector support partition
- Add ClickhouseFile sink on Spark engine
- Support compile with jdk11
- Add elasticsearch 7.x plugin on Flink engine
- Add Feishu plugin on Spark engine
- Add Spark http source plugin
- Add Clickhouse sink plugin on Flink engine

[Bugfix]
- Fix flink ConsoleSink not printing results
- Fix various jdbc type of dialect compatibility between JdbcSource and JdbcSink
- Fix when have empty data source, transform not execute
- Fix datetime/date string can't convert to timestamp/date
- Fix tableexits not contain TemporaryTable
- Fix FileSink cannot work in flink stream mode
- Fix config param issues of spark redis sink
- Fix sql parse table name error
- Fix not being able to send data to Kafka
- Fix resource lake of file.
- Fix When outputting data to doris, a ClassCastException was encountered

[Improvement]
- Change jdbc related dependency scope to default
- Use different command to execute task
- Automatic identify spark hive plugin, add enableHiveSupport
- Print config in origin order
- Remove useless job name from JobInfo
- Add console limit and batch flink fake source
- Add Flink e2e module
- Add Spark e2e module
- Optimize plugin load, rename plugin package name
- Rewrite Spark, Flink start script with code.
- To quickly locate the wrong SQL statement in flink sql transform
- Upgrade log4j version to 2.17.1
- Unified version management of third-party dependencies
- USe revision to manage project version
- Add sonar check
- Add ssl/tls parameter in spark email connector
- Remove return result of sink plugin
- Add flink-runtime-web to flink example
