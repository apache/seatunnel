package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseStructuredStreamingInput extends Plugin {


  def getDataset(spark: SparkSession): Dataset[Row]

}
