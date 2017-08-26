package org.interestinglab.waterdrop.serializer

import org.interestinglab.waterdrop.core.Event
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.Event

class Raw(config: Config) extends BaseSerializer(config) {

  // TODO : implement checkConfig
  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def checkConfig(): (Boolean, String) = (true, "")

  /**
   * Deserialize array of bytes to Event.
   */
  def deserialize(bytes: Array[Byte]): Event = {

    val e = Event()
    e.setField("raw_message", new String(bytes, this.charset))
    e
  }

  /**
   * Serialize Event to bytes of array.
   */
  def serialize(e: Event): Array[Byte] = {

    e.toString.getBytes(this.charset)
  }
}
