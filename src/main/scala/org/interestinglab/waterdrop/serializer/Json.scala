package org.interestinglab.waterdrop.serializer

import org.interestinglab.waterdrop.core.Event
import org.json4s.JsonAST.JValue

// import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson
import org.json4s.jackson.JsonMethods

object Json {

  // TODO : implement checkConfig
  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def checkConfig(): (Boolean, String) = (true, "")

  /**
   * Deserialize array of bytes to Event.
   * https://github.com/json4s/json4s/issues/316
   */
  def deserialize(bytes: Array[Byte]): Event = {

    implicit val formats = DefaultFormats
    val m = JsonMethods
      .parse(new String(bytes, "utf-8"))
      .extract[Map[String, JValue]]
    Event(m)
  }

  /**
   * Deserialize string to Event
   */
  def deserialize(str: String): Event = {

    implicit val formats = DefaultFormats
    val m = JsonMethods.parse(str).extract[Map[String, JValue]]

    Event(m)
  }

  /**
   * Serialize Event to bytes of array.
   */
  def serialize(e: Event): Array[Byte] = {

    implicit val formats = DefaultFormats
    val s = jackson.Serialization.write(e.toMap)
    s.getBytes("utf-8")
  }
}
