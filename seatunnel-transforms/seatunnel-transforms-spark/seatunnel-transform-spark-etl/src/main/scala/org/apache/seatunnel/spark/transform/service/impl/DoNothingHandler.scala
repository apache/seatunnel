package org.apache.seatunnel.spark.transform.service.impl

import org.apache.seatunnel.shade.com.typesafe.config.Config
import org.apache.seatunnel.spark.transform.service.EtlHandler
import org.apache.spark.sql.{Dataset, Row}

class DoNothingHandler extends EtlHandler {
  override def handler(df: Dataset[Row], config: Config): Dataset[Row] = df
}

object DoNothingHandler extends Serializable {
  def apply(): DoNothingHandler = new DoNothingHandler()
}
