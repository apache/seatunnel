package org.apache.seatunnel.spark.common

object Constants extends Serializable {

  final val HOST = "host"
  final val PORT = "port"
  final val AUTH = "auth"
  final val DB_NUM = "db_num"
  final val KEYS_OR_KEY_PATTERN = "keys_or_key_pattern"
  final val PARTITION_NUM = "partition_num"
  final val TIMEOUT = "timeout"
  final val DATA_TYPE = "data_type"
  final val HASH_NAME = "hash_name"
  final val SET_NAME = "set_name"
  final val ZSET_NAME = "zset_name"
  final val LIST_NAME = "list_name"
  final val RESULT_TABLE_NAME = "result_table_name"

  final val DEFAULT_HOST = "localhost"
  final val DEFAULT_PORT = 6379
  final val DEFAULT_AUTH = null
  final val DEFAULT_DB_NUM = 0
  final val DEFAULT_DATA_TYPE = "KV"
  final val DEFAULT_PARTITION_NUM = 3
  final val DEFAULT_TIMEOUT = 2000

}
