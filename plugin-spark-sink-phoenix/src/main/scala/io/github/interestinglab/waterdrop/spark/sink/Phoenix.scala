package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.phoenix.spark.ZkConnectUtil._
import scala.collection.JavaConverters._
import scala.util.Try

class Phoenix extends SparkBatchSink with Logging {

  var phoenixCfg: Map[String, String] = _
  val phoenixPrefix = "phoenix"

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    import org.apache.phoenix.spark.sparkExtend._
    data.saveToPhoenix(
      zkUrl = Some(phoenixCfg(s"$phoenixPrefix.zk-connect")),
      tableName = phoenixCfg(s"$phoenixPrefix.table"),
      tenantId = {
        if (phoenixCfg.contains(s"$phoenixPrefix.tenantId")) Some(phoenixCfg(s"$phoenixPrefix.tenantId")) else None
      },
      skipNormalizingIdentifier = {
        Try {
          if (config.hasPath("skipNormalizingIdentifier")) config.getBoolean("skipNormalizingIdentifier") else false
        }.getOrElse(false)
      })
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
