package org.apache.seatunnel.spark.transform.service

import org.apache.seatunnel.shade.com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}

trait EtlHandler extends Serializable {
  def handler(df: Dataset[Row], config: Config): Dataset[Row]
}
