package io.github.interestinglab.waterdrop.serializer

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

object Json {

  // TODO : implement checkConfig
  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def checkConfig(): (Boolean, String) = (true, "")



  /**
   * Serialize Map[String, JValue] to Json String
   */
  def serialize(e: Map[String, AnyRef]): String = {

    JSON.toJSONString(e, SerializerFeature.WriteMapNullValue)
  }
}
