package io.github.interestinglab.waterdrop.spark.stream.transform

import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingEnv
import org.apache.spark.sql.{Dataset, Row}

class Sql extends AbstractSparkStreamingTransform {

  override def process(data: Dataset[Row], env: SparkStreamingEnv): Dataset[Row] = {

    data.createOrReplaceTempView(config.getString("table.name"))
    env.sparkSession.sql(config.getString("sql"))
  }
}
