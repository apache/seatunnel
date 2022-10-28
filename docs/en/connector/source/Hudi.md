# Hudi

> Hudi source connector

## Description

Read data from Hudi.

:::tip

Engine Supported and plugin name

* [x] Spark: Hudi
* [ ] Flink

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [hoodie.datasource.read.paths](#hoodiedatasourcereadpaths) | string | yes      | -             |
| [hoodie.file.index.enable](#hoodiefileindexenable)  | boolean | no      | -             |
| [hoodie.datasource.read.end.instanttime](#hoodiedatasourcereadendinstanttime)          | string | no      | -             |
| [hoodie.datasource.write.precombine.field](#hoodiedatasourcewriteprecombinefield)            | string | no      | -             |
| [hoodie.datasource.read.incr.filters](#hoodiedatasourcereadincrfilters)       | string | no      | -             |
| [hoodie.datasource.merge.type](#hoodiedatasourcemergetype)  | string | no      | -             |
| [hoodie.datasource.read.begin.instanttime](#hoodiedatasourcereadbegininstanttime)            | string | no      | -             |
| [hoodie.enable.data.skipping](#hoodieenabledataskipping)   | string | no      | -             |
| [as.of.instant](#asofinstant)    | string | no      | -             |
| [hoodie.datasource.query.type](#hoodiedatasourcequerytype)         | string | no      | -             |
| [hoodie.datasource.read.schema.use.end.instanttime](#hoodiedatasourcereadschemauseendinstanttime)      | string | no      | -             |

Refer to [hudi read options](https://hudi.apache.org/docs/configurations/#Read-Options) for configurations.

### hoodie.datasource.read.paths

Comma separated list of file paths to read within a Hudi table.

### hoodie.file.index.enable
Enables use of the spark file index implementation for Hudi, that speeds up listing of large tables.

### hoodie.datasource.read.end.instanttime
Instant time to limit incrementally fetched data to. New data written with an instant_time <= END_INSTANTTIME are fetched out.

### hoodie.datasource.write.precombine.field
Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)

### hoodie.datasource.read.incr.filters
For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies opaque map functions, filters appearing late in the sequence of transformations cannot be automatically pushed down. This option allows setting filters directly on Hoodie Source.

### hoodie.datasource.merge.type
For Snapshot query on merge on read table, control whether we invoke the record payload implementation to merge (payload_combine) or skip merging altogetherskip_merge

### hoodie.datasource.read.begin.instanttime
Instant time to start incrementally pulling data from. The instanttime here need not necessarily correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.

### hoodie.enable.data.skipping
enable data skipping to boost query after doing z-order optimize for current table

### as.of.instant
The query instant for time travel. Without specified this option, we query the latest snapshot.

### hoodie.datasource.query.type
Whether data needs to be read, in incremental mode (new data since an instantTime) (or) Read Optimized mode (obtain latest view, based on base files) (or) Snapshot mode (obtain latest view, by merging base and (if any) log files)

### hoodie.datasource.read.schema.use.end.instanttime
Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.

## Example

```bash
hudi {
    hoodie.datasource.read.paths = "hdfs://"
}
```


