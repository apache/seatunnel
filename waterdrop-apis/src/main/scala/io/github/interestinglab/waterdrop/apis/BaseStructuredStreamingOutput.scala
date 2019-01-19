package io.github.interestinglab.waterdrop.apis

import com.typesafe.config.Config
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{Dataset, Row, SparkSession}


trait  BaseStructuredStreamingOutput extends Plugin{


  def process(df: Dataset[Row]): DataStreamWriter[Row]
}
