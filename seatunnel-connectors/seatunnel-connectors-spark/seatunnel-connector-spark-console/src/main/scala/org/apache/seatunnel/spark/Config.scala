package org.apache.seatunnel.spark

/**
 * Console configuration parameters and defaults
 */
object Config {

  /**
   * The nubmer of rows to show
   */
  val LIMIT = "limit"

  /**
   * The serializer (plain/json/schema)
   */
  val SERIALIZER = "serializer"

  /**
   * Default console show output
   */
  val PLAIN = "plain"

  /**
   * Convert dataframe to json and print
   */
  val JSON = "json"

  /**
   * Print the schema
   */
  val SCHEMA = "schema"

  /**
   * Default serializer
   */
  val DEFAULT_SERIALIZER = PLAIN

  /**
   * Default number of rows
   */
  val DEFAULT_LIMIT = 100

}
