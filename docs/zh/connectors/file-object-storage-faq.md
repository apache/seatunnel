# 文件与对象存储常见问题

本页回答 LocalFile、HDFS、S3、OSS、OBS、COS、FTP、SFTP 等文件连接器的常见问题。

## S3File 可以连接 MinIO 或其他 S3 兼容服务吗？

当目标服务兼容 Hadoop S3A 文件系统时，可以使用 `S3File`。通过 `fs.s3a.endpoint` 配置 S3A
endpoint，通过 `fs.s3a.aws.credentials.provider` 选择凭据 provider；如果服务还有特殊 Hadoop
参数要求，可放在 `hadoop_s3_properties` 中。

如果目标是有独立连接器的云对象存储，例如 OSS、OBS、COS，请以对应连接器页面中的参数和依赖
说明为准。

## 为什么一个文件 Sink 任务会产生多个文件？

文件 Sink 的输出文件数量由任务并行度和文件滚动参数共同决定：

- `batch_size` 控制每个切分文件的行数。
- `single_file_mode = true` 表示每个并行 task 输出一个文件，并且该 task 不再按 `batch_size`
  滚动。
- 当并行度大于 1 时，`single_file_mode` 不表示“整个作业只输出一个全局文件”。
- 当 `custom_filename = true` 时，自定义文件名可以在 `file_name_expression` 中使用 `${now}`
  和 `${uuid}`。

如果必须得到一个最终文件，请把该 Sink 的 `parallelism` 设为 1，或在 SeaTunnel 作业结束后由
下游流程做文件合并/压缩。

## 文件 Sink 的 save mode 如何理解？

文件 Sink 通常提供 `schema_save_mode` 和 `data_save_mode`。

- `schema_save_mode` 控制作业启动前如何准备目标路径或表 schema。
- `data_save_mode` 控制作业写入前如何处理目标路径中已有的数据文件。

精确取值和默认值请以具体 Sink 页面为准。文件系统行为还取决于连接器是否启用事务，以及目标
对象存储是否具备与 HDFS 相同的 rename/commit 语义。

## 如何把多张源表写到文件？

使用支持多表输出的 Source，并接支持多表写入的文件 Sink。对象存储路径中可以使用
`${database_name}`、`${schema_name}`、`${table_name}` 等表元信息占位符，把不同表路由到不同
目录，前提是上游确实提供了这些表元信息。

示例路径：

```hocon
sink {
  S3File {
    plugin_input = "cdc"
    path = "/warehouse/${database_name}/${table_name}/"
    file_format_type = "orc"
  }
}
```

如果上游 Source 没有提供表元信息，这些占位符无法解析。此时可以在上游增加表路由、按表拆分
作业，或使用能保留表身份的 source/transform 配置。

## 一个作业可以把多个 txt 文件写入多张 Oracle 表吗？

可以，但前提是每个输入文件能明确对应目标表，并且 schema 兼容。更易维护的选择通常是：

- 各表 schema 不同时，每张目标表一个作业；
- Source 能保留表身份、JDBC Sink 能按表路由时，使用一个多表作业；
- 原始文件需要解析、校验或补充字段时，先写入 staging 表，再进入 Oracle 目标表。

当每个文件的 schema 都不同，按表拆分作业通常更清晰，因为每个作业都能声明精确的 source
schema、transform 规则和 sink 表选项。
