package io.github.interestinglab.waterdrop.output.batch

import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import java.sql.ResultSet

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.output.utils.{ClickhouseUtil, ClickhouseUtilParam}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl, ClickHouseStatement, ClickhouseJdbcUrlParser}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

class Clickhouse extends BaseOutput {

  var tableSchema: Map[String, String] = new HashMap[String, String]()
  var jdbcLink: String = _
  var initSQL: String = _
  var table: String = _
  var localTable: String = _
  var fields: java.util.List[String] = _

  var cluster: String = _

  //contains cluster basic info
  var clusterInfo: ArrayBuffer[(String, Int, Int, String, Int)] = _
  // for distributed table, parse out the shardKey
  private val Distributed = "Distributed"
  private val rand = "rand()"
  private val brackets = ")"
  private val localTableSuffix = "_local"
  private var shardingKey: String = _

  var retryCodes: java.util.List[Integer] = _
  var config: Config = ConfigFactory.empty()
  val clickhousePrefix = "clickhouse."
  val properties: Properties = new Properties()

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

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("host", "table", "database")

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (TypesafeConfigUtils.hasSubConfig(config, clickhousePrefix)) {
      val clickhouseConfig = TypesafeConfigUtils.extractSubConfig(config, clickhousePrefix, false)
      clickhouseConfig
        .entrySet()
        .foreach(entry => {
          val key = entry.getKey
          val value = String.valueOf(entry.getValue.unwrapped())
          properties.put(key, value)
        })
    }

    if (nonExistsOptions.nonEmpty) {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string")
    }

    val hasUserName = config.hasPath("username")
    val hasPassword = config.hasPath("password")

    if (hasUserName && !hasPassword || !hasUserName && hasPassword) {
      (false, "please specify username and password at the same time")
    }
    if (hasPassword) {
      properties.put("user", config.getString("username"))
      properties.put("password", config.getString("password"))
    }

    (true, "")
  }

  override def prepare(spark: SparkSession): Unit = {
    this.jdbcLink = String.format("jdbc:clickhouse://%s/%s", config.getString("host"), config.getString("database"))

    val balanced: BalancedClickhouseDataSource = new BalancedClickhouseDataSource(this.jdbcLink, properties)
    val conn = balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]

    this.table = config.getString("table")
    this.tableSchema = getClickHouseSchema(conn, table)

    if (this.config.hasPath("fields")) {
      this.fields = config.getStringList("fields")
      val (flag, msg) = acceptedClickHouseSchema()
      if (!flag) {
        throw new ConfigRuntimeException(msg)
      }
    }

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "bulk_size" -> 20000,
        // "retry_codes" -> util.Arrays.asList(ClickHouseErrorCode.NETWORK_ERROR.code),
        "retry_codes" -> util.Arrays.asList(),
        "retry" -> 1
      )
    )

    if (config.hasPath("cluster")) {
      this.cluster = config.getString("cluster")

      this.clusterInfo = getClickHouseClusterInfo(conn, cluster)
      if (this.clusterInfo.isEmpty) {
        val errorInfo = s"cloud not find cluster config in system.clusters, config cluster = $cluster"
        logError(errorInfo)
        throw new RuntimeException(errorInfo)
      }
      logInfo(s"get [$cluster] config from system.clusters, the replica info is [$clusterInfo].")
      verifyTableEngine(table, conn)
    }

    config = config.withFallback(defaultConfig)
    retryCodes = config.getIntList("retry_codes")
    super.prepare(spark)
  }

  private def verifyTableEngine(tableName: String, conn: ClickHouseConnectionImpl): Unit = {
    val showCreateTable = s"show create table $tableName}"
    val statement: ClickHouseStatement = conn.createStatement()
    val tableDDLRS: ResultSet = statement.executeQuery(showCreateTable)
    var tableDDLString: String = ""
    while (tableDDLRS.next()) {
      tableDDLString = tableDDLRS.getString(1)
    }
    // if table use distributed engine, get the sharding key.
    if (tableDDLString.contains(Distributed)) {
      val subIndex: Int = tableDDLString.indexOf(Distributed) + Distributed.length
      val configSettings: String = tableDDLString.substring(subIndex)
      val configs: Array[String] = configSettings.split(",")
      val shardingKeyAndOtherSetting: String = configs(3)
      if (shardingKeyAndOtherSetting.contains(rand)) {
        shardingKey = rand
      } else {
        val endIndex: Int = shardingKeyAndOtherSetting.indexOf(brackets)
        val sKey: String = shardingKeyAndOtherSetting.substring(0, endIndex).trim
        shardingKey = sKey
      }
      if (config.hasPath("local_table")) {
        localTable = config.getString("local_table")
      } else {
        localTable = table.concat(localTableSuffix)
      }
    }
    statement.close()
  }

  override def process(df: Dataset[Row]): Unit = {
    val dfFields = df.schema.fieldNames
    val bulkSize = config.getInt("bulk_size")
    val retry = config.getInt("retry")

    if (!config.hasPath("fields")) {
      fields = dfFields.toList
    }

    this.initSQL = initPrepareSQL()
    logInfo(this.initSQL)

    var finalDf: Dataset[Row] = null
    if (shardingKey != null && shardingKey != rand) {
      finalDf = df.repartition(df(shardingKey))
    } else {
      finalDf = df
    }
    val param: ClickhouseUtilParam = ClickhouseUtilParam(clusterInfo, config.getString("database"), config.getString("username"), config.getString("password"), initSQL, tableSchema, fields.toList, shardingKey, bulkSize, retry, retryCodes.toList)
    finalDf.foreachPartition(partitionData => {
      val clickhouseUtil = new ClickhouseUtil(param)
      clickhouseUtil.initConnectionList()
      clickhouseUtil.add(partitionData)
      clickhouseUtil.closeConnection()
    })

  }

  private def getJDBCPort(jdbcUrl: String): Int = {
    val clickHouseProperties: ClickHouseProperties = ClickhouseJdbcUrlParser.parse(jdbcUrl, properties)
    clickHouseProperties.getPort
  }


  private def getClickHouseSchema(conn: ClickHouseConnectionImpl, table: String): Map[String, String] = {
    val sql = s"desc $table"
    val resultSet = conn.createStatement.executeQuery(sql)
    var schema = new HashMap[String, String]()
    while (resultSet.next()) {
      schema += (resultSet.getString(1) -> resultSet.getString(2))
    }
    schema
  }

  private def getClickHouseClusterInfo(
    conn: ClickHouseConnectionImpl,
    cluster: String): ArrayBuffer[(String, Int, Int, String,Int)] = {
    val sql =
      s"SELECT cluster, shard_num, shard_weight, host_address FROM system.clusters WHERE cluster = '$cluster' AND replica_num = 1"
    val resultSet = conn.createStatement.executeQuery(sql)

    val clusterInfo = ArrayBuffer[(String, Int, Int, String, Int)]()
    while (resultSet.next()) {
      val shardWeight = resultSet.getInt("shard_weight")
      for (_ <- 1 to shardWeight) {

        val custerName = resultSet.getString("cluster")
        val shardNum = resultSet.getInt("shard_num")
        val hostAddress = resultSet.getString("host_address")
        val port = getJDBCPort(jdbcLink)
        val shardInfo = Tuple5(custerName, shardNum, shardWeight, hostAddress, port)
        clusterInfo += shardInfo
      }
    }
    clusterInfo
  }

  private def initPrepareSQL(): String = {
    val prepare = List.fill(fields.size)("?")
    var finalTable = ""
    if (shardingKey != null) {
      finalTable = this.localTable
    } else {
      finalTable = this.table
    }
    val sql = String.format(
      "insert into %s (%s) values (%s)",
      finalTable,
      this.fields.map(a => s"`$a`").mkString(","),
      prepare.mkString(","))

    sql
  }

  private def acceptedClickHouseSchema(): (Boolean, String) = {

    val nonExistsFields = fields
      .map(field => (field, tableSchema.contains(field)))
      .filter { case (_, exist) => !exist }

    if (nonExistsFields.nonEmpty) {
      (
        false,
        "field " + nonExistsFields
          .map { case (option) => "[" + option + "]" }
          .mkString(", ") + " not exist in table " + this.table)
    } else {
      val nonSupportedType = fields
        .map(field => (tableSchema(field), Clickhouse.supportOrNot(tableSchema(field))))
        .filter { case (_, exist) => !exist }
      if (nonSupportedType.nonEmpty) {
        (
          false,
          "clickHouse data type " + nonSupportedType
            .map { case (option) => "[" + option + "]" }
            .mkString(", ") + " not support in current version.")
      } else {
        (true, "")
      }
    }
  }

}

object Clickhouse {

  val arrayPattern: Regex = "(Array.*)".r
  val nullablePattern: Regex = "Nullable\\((.*)\\)".r
  val lowCardinalityPattern: Regex = "LowCardinality\\((.*)\\)".r
  val intPattern: Regex = "(Int.*)".r
  val uintPattern: Regex = "(UInt.*)".r
  val floatPattern: Regex = "(Float.*)".r
  val decimalPattern: Regex = "(Decimal.*)".r

  /**
   * Waterdrop support this clickhouse data type or not.
   *
   * @param dataType ClickHouse Data Type
   * @return Boolean
   * */
  private[waterdrop] def supportOrNot(dataType: String): Boolean = {
    dataType match {
      case "Date" | "DateTime" | "String" =>
        true
      case arrayPattern(_) | nullablePattern(_) | floatPattern(_) | intPattern(_) | uintPattern(_) =>
        true
      case lowCardinalityPattern(_) =>
        true
      case decimalPattern(_) =>
        true
      case _ =>
        false
    }
  }

  private[waterdrop] def renderStringDefault(fieldType: String): String = {
    fieldType match {
      case "DateTime" =>
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        dateFormat.format(System.currentTimeMillis())
      case "Date" =>
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        dateFormat.format(System.currentTimeMillis())
      case "String" =>
        ""
    }
  }
}
