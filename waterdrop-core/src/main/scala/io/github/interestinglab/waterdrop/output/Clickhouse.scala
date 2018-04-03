package io.github.interestinglab.waterdrop.output

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

class Clickhouse (var config : Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("host", "table", "database", "fields", "bulk_size");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length == 0) {
      (true, "")
    } else {
      (false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
  }

  override def process(df: DataFrame): Unit = {
    df.foreachPartition { iter =>
      while (iter.hasNext) {
        val item = iter.next()
      }
    }
  }
}
