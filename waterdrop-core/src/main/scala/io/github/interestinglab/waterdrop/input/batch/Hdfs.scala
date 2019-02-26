package io.github.interestinglab.waterdrop.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * HDFS Static Input to read hdfs files in csv, json, parquet, parquet format.
 * */
class Hdfs extends File {

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString("path"), "hdfs://")
    fileReader(spark, path)
  }
}
