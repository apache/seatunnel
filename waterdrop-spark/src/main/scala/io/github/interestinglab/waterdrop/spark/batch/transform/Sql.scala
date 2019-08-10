package io.github.interestinglab.waterdrop.spark.batch.transform

import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

class Sql extends AbstractSparkBatchTransform {

  override def process(
      data: Dataset[Row],
      env: SparkBatchEnv
  ): Dataset[Row] = {
    data.createOrReplaceTempView(config.getString("table.name"))
    env.sparkSession.sql(config.getString("sql"))
  }

}
