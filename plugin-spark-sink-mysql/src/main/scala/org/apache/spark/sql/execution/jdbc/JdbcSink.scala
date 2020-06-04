package org.apache.spark.sql.execution.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

class JdbcSink(
                sqlContext: SQLContext,
                parameters: Map[String, String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  override def toString(): String = "JdbcSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val schema = data.schema
      val rdd: RDD[Row] = data.queryExecution.toRdd.mapPartitions { rows =>
        val converter = CatalystTypeConverters.createToScalaConverter(schema)
        rows.map(converter(_).asInstanceOf[Row])
      }
      sqlContext.createDataFrame(rdd,schema)
        .write
        .format("org.apache.spark.sql.execution.datasources.jdbc2")
        .mode(SaveMode.Append)
        .options(parameters)
        .save()
    }
  }
}
