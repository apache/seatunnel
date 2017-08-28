package org.interestinglab.waterdrop.filter

import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.interestinglab.waterdrop.core.Event

class Split(var conf: Config) extends BaseFilter(conf) {

  def checkConfig(): (Boolean, String) = {
    // TODO
    (true, "")
  }

  def prepare(sqlContext:SQLContext) {

    println("Split")
    this.sqlContext = sqlContext
  }

  //def prepare(ssc: StreamingContext) {
  //
  // set default config value
  //  if (!conf.hasPath("delimiter")) {
  //    conf = conf.withValue("delimiter", ConfigValueFactory.fromAnyRef(" "))
  //  }

  //  if (!conf.hasPath("target_field")) {
  //    conf = conf.withValue("target_field", ConfigValueFactory.fromAnyRef("__root__"))
  //  }
  //}

  def process(events: DataFrame): DataFrame = {

    events

    /*val srcField = conf.getString("source_field")
    val keys = conf.getStringList("keys")

    var isSuccess = List[Boolean]()

    for (i <- 0 until events.length) {
      for (v <- events(i).getField(srcField)) {
        val parts =
          v.asInstanceOf[String].split(conf.getString("delimiter")).map(_.trim)
        val kvs = (keys zip parts).toMap

        events(i).setField(conf.getString("target_field"), kvs)
      }

      isSuccess = true :: isSuccess
    }

    (events, isSuccess)*/
  }
}
