# Greenplum

> Greenplum数据接收器

## 描述

使用[Jdbc连接器]（Jdbc.md）将数据写入Greenplum。

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)

:::提示

不支持精确一次语义（Greenplum数据库中尚不支持XA事务）。

:::

## 选项

### driver [string]

可选jdbc驱动程序：
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

警告：为了符合许可证要求，如果您使用`GreenplumDriver`，则必须自己提供GreenplumJDBC驱动程序，例如将Greenplum-xxx.jar复制到独立版的$SEATUNNEL_HOME/lib。

### url [string]

JDBC连接的URL。如果您使用postgresql驱动程序，则值为`jdbc:postgresql://${yous_host}:${yous_port}/${yous_database}`，或者您使用greenplum驱动程序，其值为 `jdbc:pivotal:greenplum://${yous_host}:${yous_port};DatabaseName=${yous_database}`

### common 选项

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

## 更改日志

### 2.2.0-beta 2022-09-26

- 添加Greenplum写入连接器

