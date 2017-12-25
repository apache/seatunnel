package io.github.interestinglab.waterdrop.output

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import io.github.interestinglab.waterdrop.core.Plugin

abstract class BaseOutput(initConfig: Config) extends Plugin {

  def process(df: DataFrame)
}
