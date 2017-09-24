package org.interestinglab.waterdrop.output

import org.apache.spark.streaming.StreamingContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.interestinglab.waterdrop.core.Plugin

abstract class BaseOutput(config: Config) extends Plugin {

  def process(df: DataFrame)
}
