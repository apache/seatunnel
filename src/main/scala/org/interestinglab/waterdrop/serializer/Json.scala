package org.interestinglab.waterdrop.serializer

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object Json {

  // TODO : implement checkConfig
  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def checkConfig(): (Boolean, String) = (true, "")



  /**
   * Serialize Map[String, JValue] to Json String
   */
  def serialize(e: Map[String, JValue]) : String = {

    compact(e)
  }
}
