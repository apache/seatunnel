package io.github.interestinglab.waterdrop.spark.sink

object Config extends Serializable {

  val HOST = "host"
  val DATABASE = "database"
  val TABLE_NAME = "tableName"
  val USER = "user"
  val PASSWORD = "password"
  val BULK_SIZE = "bulk_size"
  val COLUMN_SEPARATOR = "column_separator"
  val ARGS_PREFIX = "doris."

  val CHARSET = "UTF-8"
  val BINARY_CT = "application/octet-stream"
  val CONTENT_TYPE = "text/plain"
  var TIMEOUT = 30000

  val CHECK_INT_ERROR = "please check bulk_size is larger than 0"
  val CHECK_USER_ERROR = "please check username and password at the same time"
  val CHECK_SUCCESS = "all check is success"

}
