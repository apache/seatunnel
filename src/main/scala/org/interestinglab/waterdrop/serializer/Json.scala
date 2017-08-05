package org.interestinglab.waterdrop.serializer

import org.interestinglab.waterdrop.core.Event
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.Event
// import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson
import org.json4s.jackson.JsonMethods


class Json(config : Config) extends BaseSerializer(config) {

    // TODO : implement checkConfig
    /**
      *  Return true and empty string if config is valid, return false and error message if config is invalid.
      */
    def checkConfig() : (Boolean, String) = (true, "")

    /**
      * Deserialize array of bytes to Event.
      */
    def deserialize(bytes : Array[Byte]) : Event = {

        implicit val formats = DefaultFormats
        val m = JsonMethods.parse(new String(bytes, this.charset)).extract[Map[String, Any]]
        Event(m)
    }

    /**
      * Serialize Event to bytes of array.
      */
    def serialize(e : Event) : Array[Byte] = {

        implicit val formats = DefaultFormats
        val s = jackson.Serialization.write(e.toMap)
        s.getBytes(this.charset)
    }
}
