package org.interestinglab.waterdrop.filter

import org.interestinglab.waterdrop.core.{Event, Plugin}
import org.apache.spark.streaming.StreamingContext
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.{Event, Plugin}
import org.interestinglab.waterdrop.core.{Event, Plugin}

abstract class BaseFilter(val config: Config) extends Plugin {

  def filter(events: List[Event]): (List[Event], List[Boolean]) = {
    val (processedEvents, isSuccess) = process(events)

    postProcess(processedEvents, isSuccess)
  }

  def prepare(ssc: StreamingContext)

  def process(events: List[Event]): (List[Event], List[Boolean])

  def postProcess(events: List[Event], isSuccess: List[Boolean]): (List[Event], List[Boolean]) = {
    val defaultTagOnFailure = "_tag"

    (events, isSuccess)
  }
}
