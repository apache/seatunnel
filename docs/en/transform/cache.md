# cache

> cache transform plugin

## Description

Supports using Cache in data integration by the transform.

:::tip

This transform **ONLY** supported by Spark.

:::

## Options

| name           | type        | required | default value |
| -------------- | ----------- | -------- | ------------- |
| storage_level       | string      | false      | -             |


### storage_level [string]

One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations. When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for iterative algorithms and fast interactive use.


NONE
DISK_ONLY
DISK_ONLY_2
MEMORY_ONLY
MEMORY_ONLY_2
MEMORY_ONLY_SER
MEMORY_ONLY_SER_2
MEMORY_AND_DISK
MEMORY_AND_DISK_2
MEMORY_AND_DISK_SER
MEMORY_AND_DISK_SER_2
OFF_HEAP

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

```bash
  
  cache {
          result_table_name="temp_cache"
   }
  
   cache {
          storage_level = "MEMORY_ONLY"
          result_table_name="temp_cache"
   }

```
