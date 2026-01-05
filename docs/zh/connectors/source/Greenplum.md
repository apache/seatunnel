import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum 源连接器

## 描述

通过 [Jdbc 连接器](Jdbc.md) 读取 Greenplum 数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

支持查询 SQL 并可以实现投影效果。

- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

:::tip

可选的 jdbc 驱动程序：
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

警告：为了符合许可证要求，如果您使用 `GreenplumDriver`，必须自己提供 Greenplum JDBC 驱动程序，例如将 greenplum-xxx.jar 复制到 $SEATUNNEL_HOME/lib（用于独立模式）。

:::

## 选项

### 通用选项

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

## 变更日志

<ChangeLog />

