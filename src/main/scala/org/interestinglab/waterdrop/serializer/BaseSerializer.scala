package org.interestinglab.waterdrop.serializer

import org.interestinglab.waterdrop.core.Plugin
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.Event
import org.interestinglab.waterdrop.core.{Event, Plugin}

// TODO : 是否需要checkConfig, prepare, 何时调用serializer的serialize, deserialize
abstract class BaseSerializer(config: Config) extends Plugin {

  val charset = if (config.hasPath("charset")) {
    config.getString("charset")
  } else {
    "utf-8"
  }

  /**
   * Deserialize array of bytes to Event.
   */
  def deserialize(bytes: Array[Byte]): Event

  /**
   * Serialize Event to bytes of array.
   */
  def serialize(e: Event): Array[Byte]
}
