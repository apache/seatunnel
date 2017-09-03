package org.interestinglab.waterdrop.filter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.interestinglab.waterdrop.core.Plugin

abstract class BaseFilter(val config: Config) extends Plugin {

  var sqlContext:SparkSession = _

  def filter(df: DataFrame, sqlContext: SparkSession): (DataFrame) = {

    prepare(sqlContext)
    process(df)
    //postProcess(processedEvents, isSuccess)
  }

  def prepare(sqlContext:SparkSession) : Unit = {

    this.sqlContext = sqlContext
  }

  def process(df: DataFrame): DataFrame

  def postProcess(df: DataFrame, isSuccess: List[Boolean]): (DataFrame, List[Boolean]) = {
    val defaultTagOnFailure = "_tag"

    (df, isSuccess)
  }
}
