package org.interestinglab.waterdrop.filter

import org.interestinglab.waterdrop.core.{Event, Plugin}
import org.apache.spark.streaming.StreamingContext
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.interestinglab.waterdrop.core.{Event, Plugin}
import org.interestinglab.waterdrop.core.{Event, Plugin}

abstract class BaseFilter(val config: Config) extends Plugin {

  var sqlContext:SQLContext = _

  def filter(events: DataFrame, sqlContext: SQLContext): (DataFrame) = {

    prepare(sqlContext)
    process(events)
    //postProcess(processedEvents, isSuccess)
  }

  def prepare(sqlContext:SQLContext)

  def process(events: DataFrame): DataFrame

  def postProcess(events: DataFrame, isSuccess: List[Boolean]): (DataFrame, List[Boolean]) = {
    val defaultTagOnFailure = "_tag"

    (events, isSuccess)
  }
}
