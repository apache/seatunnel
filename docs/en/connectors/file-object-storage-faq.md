# File and Object Storage FAQ

This page answers common questions for file connectors such as LocalFile, HDFS, S3, OSS, OBS, COS,
FTP, and SFTP.

## Can I use S3File with MinIO or another S3-compatible service?

Use `S3File` when the target is compatible with the Hadoop S3A filesystem. Configure the S3A
endpoint with `fs.s3a.endpoint`, choose the credentials provider with
`fs.s3a.aws.credentials.provider`, and add provider-specific Hadoop settings under
`hadoop_s3_properties` when required.

If your target is a native cloud object store with its own connector, such as OSS, OBS, or COS, use
the corresponding connector page for the exact options and dependencies.

## Why does one file sink task produce multiple files?

File sink output count is controlled by task parallelism and file rolling options:

- `batch_size` controls the row count for each split file.
- `single_file_mode = true` makes each parallelism task output one file and disables `batch_size`
  rolling for that task.
- `single_file_mode` does not mean "one global file for the entire job" when parallelism is greater
  than one.
- Custom file names can use `file_name_expression` with `${now}` and `${uuid}` when
  `custom_filename = true`.

If you need exactly one final file, run the sink with `parallelism = 1` or compact/merge files
downstream after the SeaTunnel job finishes.

## How do save modes work for file sinks?

File sinks commonly expose `schema_save_mode` and `data_save_mode`.

- `schema_save_mode` controls how the target path or table schema is prepared before the job starts.
- `data_save_mode` controls how existing data files in the target path are handled before writing.

For exact supported values and defaults, use the sink page for your connector. File-system behavior
also depends on whether the connector supports transactions and whether the target object store has
the same rename/commit semantics as HDFS.

## How do I write multiple source tables to files?

Use a multi-table-capable source and a file sink that supports multiple table write. In object
storage paths, table metadata placeholders such as `${database_name}`, `${schema_name}`, and
`${table_name}` can route tables to different directories when upstream table metadata exists.

Example path pattern:

```hocon
sink {
  S3File {
    plugin_input = "cdc"
    path = "/warehouse/${database_name}/${table_name}/"
    file_format_type = "orc"
  }
}
```

If the upstream source does not provide table metadata, these placeholders cannot be resolved. Add
table routing upstream, split the job by table, or use a transform/source configuration that
preserves table identity.

## Can one job load many text files into many Oracle tables?

Yes when each input can be associated with the intended table and compatible schema. The most
maintainable choices are:

- one job per target table when schemas differ;
- one multi-table job when the source preserves table identity and the JDBC sink can route by table;
- a staging-table pattern when raw files need parsing, validation, or enrichment before loading
  Oracle.

When each file has a different schema, separate jobs are usually clearer because each job can declare
the exact source schema, transform rules, and sink table options.
