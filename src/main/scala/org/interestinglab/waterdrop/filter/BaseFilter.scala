package org.interestinglab.waterdrop.filter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.interestinglab.waterdrop.core.Plugin

abstract class BaseFilter(val config: Config) extends Plugin {

  def process(spark: SparkSession, df: DataFrame): DataFrame
}
