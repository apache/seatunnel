package org.interestinglab.waterdrop.output

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.interestinglab.waterdrop.core.Plugin

abstract class BaseOutput(initConfig: Config) extends Plugin {

  def process(df: DataFrame)
}
