package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory}

class Fake extends SparkBatchSource {


  override def prepare(env: SparkEnvironment): Unit = {}

  override def getData(env: SparkEnvironment): Dataset[Row] = {

    val s = Seq(RowFactory.create("Hello garyelephant"),
                RowFactory.create("Hello rickyhuo"),
                RowFactory.create("Hello kid-xiong"))

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    env.getSparkSession.createDataset(s)(RowEncoder(schema))
  }

  override def checkConfig(): CheckResult = {
    new CheckResult(true, "")
  }
}
