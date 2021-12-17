package io.github.interestinglab.waterdrop.spark.sink
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}
import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.ListBuffer

class Doris extends SparkBatchSink with Serializable {

  var apiUrl: String = _
  var column_separator: String = "\t"
  var propertiesMap = new mutable.HashMap[String,String]()

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val bulkSize: Int = config.getInt(Config.BULK_SIZE)
    val user: String = config.getString(Config.USER)
    val password: String = config.getString(Config.PASSWORD)
    if (propertiesMap.contains(Config.COLUMN_SEPARATOR)) {
     column_separator =  propertiesMap(Config.COLUMN_SEPARATOR)
    }
    val sparkSession = env.getSparkSession
    import sparkSession.implicits._
    val dataFrame = data.map(x => x.toString().replaceAll("\\[|\\]", "").replace(",", column_separator))
    dataFrame.foreachPartition { partition =>
      var count: Int = 0
      val buffer = new ListBuffer[String]
      val dorisUtil = new DorisUtil(propertiesMap.toMap, apiUrl, user, password)
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
      val host: String = config.getString(Config.HOST)
      val dataBase: String = config.getString(Config.DATABASE)
      val tableName: String = config.getString(Config.TABLE_NAME)
      this.apiUrl = s"http://$host/api/$dataBase/$tableName/_stream_load"
      new CheckResult(true,Config.CHECK_SUCCESS)
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val httpConfig = JavaConversions.asScalaSet(config.entrySet()).filter(x => x.getKey.startsWith(Config.ARGS_PREFIX))
    if (httpConfig.nonEmpty) {
      httpConfig.foreach(tuple => {
        val split = tuple.getKey.split(".")
        if (split.size == 2) {
          propertiesMap += (split(0) -> tuple.getValue.render())
        }
      })
    }
  }
}
