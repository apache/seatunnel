package io.github.interestinglab.waterdrop.serializer

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.core.Plugin


// TODO : 是否需要checkConfig, prepare, 何时调用serializer的serialize, deserialize
abstract class BaseSerializer(config: Config) extends Plugin {

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
