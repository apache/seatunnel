package io.github.interestinglab.waterdrop.serializer

import io.github.interestinglab.waterdrop.config.Config


// TODO : 是否需要checkConfig, prepare, 何时调用serializer的serialize, deserialize
abstract class BaseSerializer(config: Config) {

  val charset = if (config.hasPath("charset")) {
    config.getString("charset")
  } else {
    "utf-8"
  }

  /**
   * Deserialize array of bytes to String.
   */
  def deserialize(bytes: Array[Byte]): String

  /**
   * Serialize String to bytes of array.
   */
  def serialize(e: String): Array[Byte]
}
