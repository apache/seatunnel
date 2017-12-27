package io.github.interestinglab.waterdrop.apis

import com.typesafe.config.Config
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseFilter(val initConfig: Config) extends Plugin {

  def process(spark: SparkSession, df: DataFrame): DataFrame

  /**
   * Allow to register user defined UDFs
   * @return empty list if there is no UDFs to be registered
   * */
  def getUdfList(): List[(String, UserDefinedFunction)] = List.empty

  /**
   * Allow to register user defined UDAFs
   * @return empty list if there is no UDAFs to be registered
   * */
  def getUdafList(): List[(String, UserDefinedAggregateFunction)] = List.empty
}
