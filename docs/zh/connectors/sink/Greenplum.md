import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum Sink 连接器

## 描述

使用 [JDBC 连接器](Jdbc.md) 将数据写入 Greenplum。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

:::tip

不支持精确一次语义（Greenplum 数据库尚不支持 XA 事务）。

:::

## 选项

### driver [string]

可选的 JDBC 驱动程序：
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

警告：为了符合许可证要求，如果您使用 `GreenplumDriver`，则必须自己提供 Greenplum JDBC 驱动程序，例如将 greenplum-xxx.jar 复制到 $SEATUNNEL_HOME/lib（用于独立模式）。

### url [string]

JDBC 连接的 URL。如果使用 PostgreSQL 驱动程序，值为 `jdbc:postgresql://${yous_host}:${yous_port}/${yous_database}`，或者如果使用 Greenplum 驱动程序，值为 `jdbc:pivotal:greenplum://${yous_host}:${yous_port};DatabaseName=${yous_database}`

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见。

## 变更日志

<ChangeLog />

