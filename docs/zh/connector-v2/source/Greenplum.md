# Greenplum

> Greenplum 源连接器

## 描述

读取 Greenplum 数据,通过 [Jdbc 连接器](Jdbc.md).

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)

支持查询SQL，可以实现投影效果.

- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

:::提示

可选jdbc驱动程序:
- `org.postgresql.Driver`
- `com.pivotal.jdbc.GreenplumDriver`

警告：为了符合许可证要求, 如果您使用 `GreenplumDriver` 则必须自己提供 Greenplum JDBC 驱动程序, 例如将 greenplum-xxx.jar 复制到单独的 $SEATUNNEL_HOME/lib 下.

:::

## 选项

### 常见选项

源插件常用参数，详见 [Source Common Options](../source-common-options.md) .

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加 Greenplum 源连接器

