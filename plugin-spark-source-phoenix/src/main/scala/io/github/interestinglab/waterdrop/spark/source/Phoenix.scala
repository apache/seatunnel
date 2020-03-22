package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.phoenix.spark.ZkConnectUtil._
import scala.collection.JavaConverters._

class Phoenix extends SparkBatchSource with Logging {

  var phoenixCfg: Map[String, String] = _
  val phoenixPrefix = "phoenix"

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    import org.apache.phoenix.spark.sparkExtend._
    env.getSparkSession.sqlContext.phoenixTableAsDataFrame(
      table = phoenixCfg(s"$phoenixPrefix.table"),
      columns = {
        if (config.hasPath("columns")) config.getStringList("columns").asScala else Seq()
      },
      zkUrl = Some(phoenixCfg(s"$phoenixPrefix.zk-connect")),
      predicate = if (phoenixCfg.contains(s"$phoenixPrefix.predicate")) Some(phoenixCfg(s"$phoenixPrefix.predicate")) else None,
      tenantId = if (phoenixCfg.contains(s"$phoenixPrefix.tenantId")) Some(phoenixCfg(s"$phoenixPrefix.tenantId")) else None
    )
  }

  override def checkConfig(): CheckResult = {
    if (config.hasPath("zk-connect") && config.hasPath("table") && StringUtils.isNotBlank(config.getString("zk-connect"))) {
      checkZkConnect(config.getString("zk-connect"))
      new CheckResult(true, "")
    } else {
      new CheckResult(false, "please specify [zk-connect] as a non-empty string")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    phoenixCfg = config.entrySet().asScala.map {
      entry => s"$phoenixPrefix.${entry.getKey}" -> String.valueOf(entry.getValue.unwrapped())
    }.toMap

    printParams()
  }

  def printParams(): Unit = {
    phoenixCfg.foreach {
      case (key, value) => logInfo("[INFO] \t" + key + " = " + value)
    }
  }

}
