package org.interestinglab.waterdrop.core

import scala.util.control.Breaks._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


class Event(protected var e : Map[String, JValue]) {

    override def toString : String = e.toString

    // TODO : implement toJSON
    def toJSON() : String = {

        compact(e)
    }

    def toMap() : Map[String, Any] = e

    def setField(key : String, value: Any) : Unit = {

        setField(key, Event.toJValue(value))
    }


    def setField(key : String, value : JValue) : Unit = {

        if (key == "__root__") {
           // value must be a Map
           value match {
               case m : Map[String @unchecked, JValue @unchecked] => e = e ++ m
               case _ => return
           }
        }

        val splitedKeys = key.split("\\.")

        var v : JValue = value
        for(i <- 1 until splitedKeys.length) {
            val prefixKey = splitedKeys.slice(0, splitedKeys.length - i)
            val curKey = splitedKeys.slice(splitedKeys.length - i, splitedKeys.length - i + 1)(0)

            v = getField(prefixKey.mkString(".")) match {
                case Some(m : Map[String @unchecked, JValue @unchecked]) => m + (curKey -> v)
                case _ => Map[String, JValue](curKey -> v)
            }
        }

        e = e + (splitedKeys.slice(0, 1)(0) ->v)
    }

    def getField(key : String) : Option[Any] = {

        val keyPath = key.split("\\.")
        getField(keyPath)
    }

    def getField(keyPath : Array[String]) : Option[Any] = {

        var value = None : Option[Any]

        var itM = e
        breakable {
            for(i <- 0 until keyPath.length) {

                val k = keyPath(i)

                if (i == keyPath.length - 1) {

                    value = Some(itM.getOrElse(k, None))
                }
                else {
                    itM.getOrElse(k, None) match {
                        case m: Map[String @unchecked, JValue @unchecked] => itM = m
                        case _ => value = None; break
                    }
                }
            }
        }

        value
    }

}


object Event {

    def apply() : Event = {
        val e = Map[String, JValue]()
        new Event(e)
    }

    def apply(e : Map[String, JValue]) : Event = {
        new Event(e)
    }

    /**
      * Create Event using json string
      */
    def apply(json : String) : Event = {
        // TODO : implement creating Event by json string
        val e = Map[String, JValue]()
        new Event(e)
    }

    /**
     * How or where to locate this function
     */
    def toJValue(str : Any) : JValue = {

        import org.json4s.{ Extraction, NoTypeHints }
        import org.json4s.jackson.Serialization
        implicit val formats = Serialization.formats(NoTypeHints)

        Extraction.decompose(str)
    }

}
