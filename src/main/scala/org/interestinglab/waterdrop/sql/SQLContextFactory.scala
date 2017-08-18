package org.interestinglab.waterdrop.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Singleton to get SQLContext instance.
  * Using SparkSession instead of SQLContext since spark2.0.0
  */
object SQLContextFactory {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
