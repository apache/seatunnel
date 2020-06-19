package io.github.interestinglab.waterdrop.input.batch

import com.hortonworks.hwc.HiveWarehouseSession
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive3 extends BaseStaticInput {
  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("sql") match {
      case true => (true, "")
      case false => (false, "please specify [sql]")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val sql = config.getString("sql")
    val hive = HiveWarehouseSession.session(spark).build()
    val df=hive.sql(sql)
    hive.close()
    df
  }
}
