package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

trait BaseStructuredStreamingOutput extends ForeachWriter[Row] with BaseStructuredStreamingOutputIntra {

  /**
   * Things to do before process.
   * */
  def open(partitionId: Long, epochId: Long): Boolean

  /**
   * Things to do with each Row.
   * */
  def process(row: Row): Unit

  /**
   * Things to do after process.
   * */
  def close(errorOrNull: Throwable): Unit

  /**
   * Waterdrop Structured Streaming process.
   * */
  def process(df: Dataset[Row]): DataStreamWriter[Row]
}
