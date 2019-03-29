package io.github.interestinglab.waterdrop.input.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}

/**
 * This is a example static input that can be demonstrated for static input.
 * */
class Fake extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  /**
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val s = Seq(
      RowFactory.create("Hello garyelephant"),
      RowFactory.create("Hello rickyhuo"),
      RowFactory.create("Hello kid-xiong"))

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    spark.createDataset(s)(RowEncoder(schema))
  }
}
