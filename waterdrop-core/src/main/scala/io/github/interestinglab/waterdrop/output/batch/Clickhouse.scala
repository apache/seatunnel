package io.github.interestinglab.waterdrop.output.batch

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.yandex.clickhouse.except.{ClickHouseException, ClickHouseUnknownException}
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl, ClickHousePreparedStatement}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable.WrappedArray
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class Clickhouse extends BaseOutput {

  var tableSchema: Map[String, String] = new HashMap[String, String]()
  var jdbcLink: String = _
  var initSQL: String = _
  var table: String = _
  var fields: java.util.List[String] = _
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

    val requiredOptions = List("host", "table", "database", "fields")

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
          .mkString(", ") + " as non-empty string"
      )
    } else if (config.hasPath("username") && !config.hasPath("password") || config.hasPath("password")
      && !config.hasPath("username")) {
      (false, "please specify username and password at the same time")
    } else {

      this.jdbcLink = String.format("jdbc:clickhouse://%s/%s", config.getString("host"), config.getString("database"))

      if (config.hasPath("username")) {
        properties.put("user", config.getString("username"))
        properties.put("password", config.getString("password"))
      }

      val balanced: BalancedClickhouseDataSource = new BalancedClickhouseDataSource(jdbcLink, properties)
      val conn = balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]

      this.table = config.getString("table")
      this.tableSchema = getClickHouseSchema(conn, table)
      this.fields = config.getStringList("fields")

      acceptedClickHouseSchema()
    }
  }

  override def prepare(spark: SparkSession): Unit = {

    this.initSQL = initPrepareSQL()
    logInfo(this.initSQL)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "bulk_size" -> 20000,
        // "retry_codes" -> util.Arrays.asList(ClickHouseErrorCode.NETWORK_ERROR.code),
        "retry_codes" -> util.Arrays.asList(),
        "retry" -> 1
      )
    )
    config = config.withFallback(defaultConfig)
    retryCodes = config.getIntList("retry_codes")
    super.prepare(spark)
  }

  override def process(df: Dataset[Row]): Unit = {
    val dfFields = df.schema.fieldNames
    val bulkSize = config.getInt("bulk_size")
    val retry = config.getInt("retry")
    df.foreachPartition { iter =>
      val executorBalanced = new BalancedClickhouseDataSource(this.jdbcLink, this.properties)
      val executorConn = executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
      val statement = executorConn.createClickHousePreparedStatement(this.initSQL)
      var length = 0
      while (iter.hasNext) {
        val item = iter.next()
        length += 1
        renderStatement(fields, item, dfFields, statement)
        statement.addBatch()

        if (length >= bulkSize) {
          execute(statement, retry)
          length = 0
        }
      }

      execute(statement, retry)
    }
  }

  private def execute(statement: ClickHousePreparedStatement, retry: Int): Unit = {
    val res = Try(statement.executeBatch())
    res match {
      case Success(_) => logInfo("Insert into ClickHouse succeed")
      case Failure(e: ClickHouseException) => {
        val errorCode = e.getErrorCode
        if (retryCodes.contains(errorCode)) {
          logError("Insert into ClickHouse failed. Reason: ", e)
          if (retry > 0) {
            execute(statement, retry - 1)
          } else {
            logError("Insert into ClickHouse failed and retry failed, drop this bulk.")
          }
        } else {
          throw e
        }
      }
      case Failure(e: ClickHouseUnknownException) => {
        throw e
      }
      case Failure(e: Exception) => {
        throw e
      }
    }
  }

  private def getClickHouseSchema(conn: ClickHouseConnectionImpl, table: String): Map[String, String] = {
    val sql = String.format("desc %s", table)
    val resultSet = conn.createStatement.executeQuery(sql)
    var schema = new HashMap[String, String]()
    while (resultSet.next()) {
      schema += (resultSet.getString(1) -> resultSet.getString(2))
    }
    schema
  }

  private def initPrepareSQL(): String = {
    val prepare = List.fill(fields.size)("?")
    val sql = String.format(
      "insert into %s (%s) values (%s)",
      this.table,
      this.fields.map(a => a).mkString(","),
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

  private def renderDefaultStatement(index: Int, fieldType: String, statement: ClickHousePreparedStatement): Unit = {
    fieldType match {
      case "DateTime" | "Date" | "String" =>
        statement.setString(index + 1, Clickhouse.renderStringDefault(fieldType))
      case "Int8" | "UInt8" | "Int16" | "Int32" | "UInt32" | "UInt16" =>
        statement.setInt(index + 1, 0)
      case "UInt64" | "Int64" =>
        statement.setLong(index + 1, 0)
      case "Float32" => statement.setFloat(index + 1, 0)
      case "Float64" => statement.setDouble(index + 1, 0)
      case Clickhouse.arrayPattern(_) => statement.setArray(index + 1, List())
      case Clickhouse.nullablePattern(nullFieldType) => renderNullStatement(index, nullFieldType, statement)
      case _ => statement.setString(index + 1, "")
    }
  }

  private def renderNullStatement(index: Int, fieldType: String, statement: ClickHousePreparedStatement): Unit = {
    fieldType match {
      case "String" =>
        statement.setNull(index + 1, java.sql.Types.VARCHAR)
      case "DateTime" => statement.setNull(index + 1, java.sql.Types.DATE)
      case "Date" => statement.setNull(index + 1, java.sql.Types.TIME)
      case "Int8" | "UInt8" | "Int16" | "Int32" | "UInt32" | "UInt16" =>
        statement.setNull(index + 1, java.sql.Types.INTEGER)
      case "UInt64" | "Int64" =>
        statement.setNull(index + 1, java.sql.Types.BIGINT)
      case "Float32" => statement.setNull(index + 1, java.sql.Types.FLOAT)
      case "Float64" => statement.setNull(index + 1, java.sql.Types.DOUBLE)
    }
  }

  private def renderBaseTypeStatement(
    index: Int,
    field: String,
    fieldType: String,
    item: Row,
    statement: ClickHousePreparedStatement): Unit = {
    fieldType match {
      case "DateTime" | "Date" | "String" =>
        statement.setString(index + 1, item.getAs[String](field))
      case "Int8" | "UInt8" | "Int16" | "UInt16" | "Int32" =>
        statement.setInt(index + 1, item.getAs[Int](field))
      case "UInt32" | "UInt64" | "Int64" =>
        statement.setLong(index + 1, item.getAs[Long](field))
      case "Float32" => statement.setFloat(index + 1, item.getAs[Float](field))
      case "Float64" => statement.setDouble(index + 1, item.getAs[Double](field))
      case Clickhouse.arrayPattern(_) =>
        statement.setArray(index + 1, item.getAs[WrappedArray[AnyRef]](field))
      case _ => statement.setString(index + 1, item.getAs[String](field))
    }
  }

  private def renderStatement(
    fields: util.List[String],
    item: Row,
    dsFields: Array[String],
    statement: ClickHousePreparedStatement): Unit = {
    for (i <- 0 until fields.size()) {
      val field = fields.get(i)
      val fieldType = tableSchema(field)
      if (dsFields.indexOf(field) == -1) {
        renderDefaultStatement(i, fieldType, statement)
      } else {
        fieldType match {
          case "String" | "DateTime" | "Date" | Clickhouse.arrayPattern(_) =>
            renderBaseTypeStatement(i, field, fieldType, item, statement)
          case Clickhouse.floatPattern(_) | Clickhouse.intPattern(_) | Clickhouse.uintPattern(_) =>
            renderBaseTypeStatement(i, field, fieldType, item, statement)
          case Clickhouse.nullablePattern(nullType) =>
            renderBaseTypeStatement(i, field, nullType, item, statement)
          case _ => statement.setString(i + 1, item.getAs[String](field))
        }
      }
    }
  }
}

object Clickhouse {

  val arrayPattern: Regex = "(Array.*)".r
  val nullablePattern: Regex = "Nullable\\((.*)\\)".r
  val intPattern: Regex = "(Int.*)".r
  val uintPattern: Regex = "(UInt.*)".r
  val floatPattern: Regex = "(Float.*)".r

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
