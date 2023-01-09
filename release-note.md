# Next-release

## New Feature
### Zeta Engine
- [Script]Add support close engine instance shell
### Core
- [Starter][Flink]Support transform-v2 for flink #3396
### Transformer
- [Spark] Support transform-v2 for spark (#3409)
- [ALL]Add FieldMapper Transform #3781

## Improves
### Connectors
- [CDC]Add mysql-cdc source factory #3791
- [JDBC]Fix the problem that the exception cannot be thrown normally #3796
- [JDBC]Remove unused options for jdbc source factory #3794
- [JDBC]Add exactly-once for JDBC source connector #3750
- [JDBC]Fix JDBC source xa transaction commit failure on pipeline restore #3809
- [JDBC]Improve option check rule
- [JDBC]Support SAP HANA. (#3017)
- [MongoDB]Add source query capability #3697
- [File]Fix file source connector option rule bug #3804
- [File]Add lzo compression way
- [Kafka]Fix Source failed to parse offset format #3810
- [Kafka]Fix source the default value of commit_on_checkpoint #3831
- [Kafka & RabbitMQ & StarRocks & ClickHouse]Change Connector Custom Config Prefix To Map #3719
- [Common]The log outputs detailed exception stack information #3805
- [API]Add parallelism and column projection interface #3829
- [API]Add get source method to all source connector #3846
### Zeta Engine
- [Chore] Remove unnecessary dependencies #3795
- [Core] Improve job restart of all node down #3784
- [Checkpoint] Cancel CheckpointCoordinator First Before Cancel Task #3838
- [Storage] Remove seatunnel-api from engine storage. #3834

## Bug Fixes
### Connectors
- [ClickHouse File] Fix ClickhouseFile Committer Serializable Problems #3803

### Zeta Engine
- [Checkpoint] Fix Checkpoint Continue Trigger After Job CANCELED #3808
- [Core]Fix Local Mode can't deserialize split (#3817)
- [Metrics] Fix Metrics will lose when Job be canceled. #3797


## Test
### E2E
- [SqlServer CDC] fix SqlServerCDC IT failure #3807


