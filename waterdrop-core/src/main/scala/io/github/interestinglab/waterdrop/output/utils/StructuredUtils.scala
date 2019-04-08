package io.github.interestinglab.waterdrop.output.utils

import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

object StructuredUtils {

  def setCheckpointLocation(dw: DataStreamWriter[Row], config: Config): DataStreamWriter[Row] = {
    if (config.hasPath("checkpointLocation")) {
      dw.option("checkpointLocation", config.getString("checkpointLocation"))
    } else {
      dw
    }
  }

  def checkTriggerMode(config: Config): Boolean = {
    config.hasPath("triggerMode") match {
      case true => {
        val triggerMode = config.getString("triggerMode")
        triggerMode match {
          case "ProcessingTime" | "Continuous" => {
            if (config.hasPath("interval")) {
              true
            } else {
              false
            }
          }
          case _ => true
        }
      }
      case false => true
    }
  }

  def writeWithTrigger(config: Config, writer: DataStreamWriter[Row]): DataStreamWriter[Row] = {
    config.getString("trigger_type") match {
      case "default" => writer
      case "ProcessingTime" => writer.trigger(Trigger.ProcessingTime(config.getString("interval")))
      case "OneTime" => writer.trigger(Trigger.Once())
      case "Continuous" => writer.trigger(Trigger.Continuous(config.getString("interval")))
    }
  }
}
