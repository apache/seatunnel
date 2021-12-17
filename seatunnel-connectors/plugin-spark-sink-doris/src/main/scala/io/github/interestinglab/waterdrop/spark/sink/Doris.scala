package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer

class Doris extends SparkBatchSink with Serializable {

  var apiUrl: String = _
  var user: String = _
  var password: String = _
  var propertiesMap: Map[String,String] = _

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val bulkSize: Int = config.getInt(Config.BULK_SIZE)
    val column_separator: String = propertiesMap.getOrElse("column_separator","\t")
    if (!propertiesMap.contains("columns")) {
      val fields = data.schema.fieldNames.mkString(",")
      propertiesMap += ("columns" -> fields)
    }
    val sparkSession = env.getSparkSession
    import sparkSession.implicits._
    val dataFrame = data.map(x => x.toString().replaceAll("\\[|\\]", "").replace(",", column_separator))
    dataFrame.foreachPartition { partition =>
      var count: Int = 0
      val buffer = new ListBuffer[String]
      val dorisUtil = new DorisUtil(propertiesMap, apiUrl, user, password)
      for (message <- partition) {
        count += 1
        buffer += message
        if (count > bulkSize) {
          dorisUtil.saveMessages(buffer.mkString("\n"))
          buffer.clear()
          count = 0
        }
      }
      dorisUtil.saveMessages(buffer.mkString("\n"))
    }
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List(Config.HOST, Config.DATABASE, Config.TABLE_NAME)
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }
    if (nonExistsOptions.nonEmpty) {
      new CheckResult(false, "please specify " + nonExistsOptions
        .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }.mkString(", ") + " as non-empty string"
      )
    } else if (config.hasPath(Config.USER) && !config.hasPath(Config.PASSWORD) || config.hasPath(Config.PASSWORD) && !config.hasPath(Config.USER)) {
      new CheckResult(false, Config.CHECK_USER_ERROR)
    } else if (config.hasPath(Config.BULK_SIZE) && config.getInt(Config.BULK_SIZE) < 0) {
      new CheckResult(false,Config.CHECK_INT_ERROR)
    } else {
      this.apiUrl = s"http://${Config.HOST}/api/${Config.DATABASE}/${Config.TABLE_NAME}/_stream_load"
      new CheckResult(true,Config.CHECK_SUCCESS)
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    config match {
      case _ if config.hasPath(Config.COLUMN_SEPARATOR) =>
        propertiesMap += (Config.COLUMN_SEPARATOR -> config.getString(Config.COLUMN_SEPARATOR))
      case _ if config.hasPath(Config.MAX_FILTER_RATIO) =>
        propertiesMap += (Config.MAX_FILTER_RATIO -> config.getString(Config.MAX_FILTER_RATIO))
      case _ if config.hasPath(Config.PARTITION) =>
        propertiesMap += (Config.PARTITION -> config.getString(Config.PARTITION))
      case _ if config.hasPath(Config.COLUMNS) =>
        propertiesMap += (Config.COLUMNS -> config.getString(Config.COLUMNS))
      case _ if config.hasPath(Config.EXEC_MEM_LIMIT) =>
        propertiesMap += (Config.EXEC_MEM_LIMIT -> config.getString(Config.EXEC_MEM_LIMIT))
      case _ if config.hasPath(Config.STRICT_MODE) =>
        propertiesMap += (Config.STRICT_MODE -> config.getString(Config.STRICT_MODE))
      case _ if config.hasPath(Config.MERGE_TYPE) =>
        propertiesMap += (Config.MERGE_TYPE -> config.getString(Config.MERGE_TYPE))
    }
  }
}
