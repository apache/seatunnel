package io.github.interestinglab.waterdrop.core

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
 * checkConfig --> prepare
 */
abstract class Plugin extends Serializable with Logging {

  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def checkConfig(): (Boolean, String)

  /**
   * Get Plugin Name.
   */
  def name: String = this.getClass.getName

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {}
}
