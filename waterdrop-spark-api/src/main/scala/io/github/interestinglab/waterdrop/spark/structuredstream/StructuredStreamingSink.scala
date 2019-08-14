package io.github.interestinglab.waterdrop.spark.structuredstream

import io.github.interestinglab.waterdrop.spark.BaseSparkSink
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.Row

trait StructuredStreamingSink extends BaseSparkSink[DataStreamWriter[Row]]{

  
}
