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
| storage_level       | string      | false      | MEMORY_ONLY          |


### storage_level [string]

One of the most important capabilities in Spark is persisting (or caching) a dataset in memory across operations. When you persist an RDD, each node stores any partitions of it that it computes in memory and reuses them in other actions on that dataset (or datasets derived from it). This allows future actions to be much faster (often by more than 10x). Caching is a key tool for iterative algorithms and fast interactive use.

| Storage Level           | Meaning |
| --------------          | ------------- |
| MEMORY_ONLY             | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level.|
| MEMORY_AND_DISK             | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed.|
| MEMORY_ONLY_SER (Java and Scala)     | Store RDD as serialized Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a fast serializer, but more CPU-intensive to read.|
| MEMORY_AND_DISK_SER (Java and Scala)   | Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed.|
| DISK_ONLY             | Store the RDD partitions only on disk.|
| MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.             | Same as the levels above, but replicate each partition on two cluster nodes.|
| OFF_HEAP (experimental)	 | Similar to MEMORY_ONLY_SER, but store the data in off-heap memory. This requires off-heap memory to be enabled.|

For more details, please refer to [https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#rdd-persistence]


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
