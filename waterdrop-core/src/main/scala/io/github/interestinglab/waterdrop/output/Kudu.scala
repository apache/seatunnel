package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.{BaseOutput}
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{Dataset, Row}


class Kudu extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("kudu_master") && config.hasPath("kudu_table") && config.hasPath("table_name") match {
      case true => (true, "")
      case false => (false, "please specify [kudu_master] and [kudu_table] and [table_name]")
    }
  }


  override def process(df: Dataset[Row]): Unit = {

    val kuduContext = new KuduContext(config.getString("kudu_master"),df.sparkSession.sparkContext)

    kuduContext.upsertRows(df,config.getString("kudu_table"))

  }

}
