package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Superclass of all static input, extends this abstract class to implement a static input.
 * */
abstract class BaseStaticInput extends Plugin {

  /**
   *
   * */
  def createDataset(spark: SparkSession): Dataset[Row]
}
