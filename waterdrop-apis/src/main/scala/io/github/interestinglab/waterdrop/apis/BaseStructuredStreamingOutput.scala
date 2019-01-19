package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row}


trait BaseStructuredStreamingOutput extends Plugin{
  def process(df: Dataset[Row]): DataStreamWriter[Row]
}
