package org.interestinglab.waterdrop.filter

import org.interestinglab.waterdrop.core.Event
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.interestinglab.waterdrop.core.Event


class Split(var conf : Config) extends BaseFilter(conf) {

    def checkConfig() : (Boolean, String) = {
        // TODO
        (true, "")
    }

    def prepare(ssc : StreamingContext) {

        // set default config value
        if (!conf.hasPath("delimiter")) {
            conf = conf.withValue("delimiter", ConfigValueFactory.fromAnyRef(" "))
        }

        if (!conf.hasPath("target_field")) {
            conf = conf.withValue("target_field", ConfigValueFactory.fromAnyRef("__root__"))
        }
    }

    def process(events : List[Event]) : (List[Event], List[Boolean]) = {

        val srcField = conf.getString("source_field")
        val keys = conf.getStringList("keys")

        var isSuccess = List[Boolean]()

        for (i <- 0 until events.length) {
            for (v <- events(i).getField(srcField)) {
                val parts = v.asInstanceOf[String].split(conf.getString("delimiter")).map(_.trim)
                val kvs = (keys zip parts).toMap

                events(i).setField(conf.getString("target_field"), kvs)
            }

            isSuccess = true :: isSuccess
        }

        (events, isSuccess)
    }
}
