package io.github.interestinglab.waterdrop.output.batch

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession
import io.github.interestinglab.waterdrop.apis.{BaseOutput, BaseStaticInput}
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive3 extends BaseOutput{
  var config: Config = ConfigFactory.empty()
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("table") match {
      case true => (true, "")
      case false => (false, "please specify [table]")
    }
  }

  override def process(df: Dataset[Row]): Unit = {
    val saveMode = if(this.config.hasPath("save_mode"))this.config.getString("save_mode") else "append"
    val table = this.config.getString("table")
    val partition = if (config.hasPath("partition")) this.config.getString("partition") else null
    if(partition!=null){
      df.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).mode(saveMode).option("table", table).option("partition", partition).save()
    }else{
      df.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).mode(saveMode).option("table", table).save()
    }
  }
}
