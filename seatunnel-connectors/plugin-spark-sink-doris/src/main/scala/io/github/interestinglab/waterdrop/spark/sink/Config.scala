package io.github.interestinglab.waterdrop.spark.sink

object Config {

  val HOST = "host"
  val DATABASE = "database"
  val TABLE_NAME = "tableName"
  val USER = "user"
  val PASSWORD = "password"
  val BULK_SIZE = "bulk_size"
  val COLUMNS = "columns"
  val COLUMN_SEPARATOR = "column_separator"
  val MAX_FILTER_RATIO = "max_filter_ratio"
  val PARTITION = "partition"
  val EXEC_MEM_LIMIT = "exec_mem_limit"
  val STRICT_MODE = "STRICT_MODE"
  val MERGE_TYPE = "merge_type"

  val CHECK_INT_ERROR = "please check bulk_size is larger than 0"
  val CHECK_USER_ERROR = "please check username and password at the same time"
  val CHECK_SUCCESS = "all check is success"

}
