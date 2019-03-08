package io.github.interestinglab.waterdrop.output.structuredstreaming

import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamWriter

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
}
