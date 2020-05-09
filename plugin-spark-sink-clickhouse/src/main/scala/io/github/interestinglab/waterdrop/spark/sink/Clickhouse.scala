package io.github.interestinglab.waterdrop.spark.sink

import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import java.math.BigDecimal
import java.sql.PreparedStatement

import io.github.interestinglab.waterdrop.config.ConfigFactory
import io.github.interestinglab.waterdrop.common.config.{CheckResult, TypesafeConfigUtils}
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.yandex.clickhouse.except.{ClickHouseException, ClickHouseUnknownException}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}


class Clickhouse extends SparkBatchSink {

  var tableSchema: Map[String, String] = new HashMap[String, String]()
  var jdbcLink: String = _
  var initSQL: String = _
  var table: String = _
  var fields: java.util.List[String] = _
  var retryCodes: java.util.List[Integer] = _
  //  var config: Config = ConfigFactory.empty()
  val clickhousePrefix = "clickhouse."
  val properties: Properties = new Properties()

  override def output(data: Dataset[Row], environment: SparkEnvironment): Unit = {
    val dfFields = data.schema.fieldNames
    val bulkSize = config.getInt("bulk_size")
    val retry = config.getInt("retry")

    if (!config.hasPath("fields")) {
      fields = dfFields.toList
      initSQL = initPrepareSQL()
    }
    data.foreachPartition { iter =>
      val executorBalanced = new BalancedClickhouseDataSource(this.jdbcLink, this.properties)
      val executorConn = executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
      val statement = executorConn.prepareStatement(this.initSQL)

      var length = 0

      for (item <- iter) {
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

  override def checkConfig(): CheckResult = {
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
      new CheckResult(
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
      new CheckResult(false, "please specify username and password at the same time")
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

      if (this.config.hasPath("fields")) {
        this.fields = config.getStringList("fields")
        acceptedClickHouseSchema()
      } else {
        new CheckResult(true, "")
      }
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    if (config.hasPath("fields")) {
      this.initSQL = initPrepareSQL()
    }

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

  private def acceptedClickHouseSchema(): CheckResult = {
    val nonExistsFields = fields
      .map(field => (field, tableSchema.contains(field)))
      .filter { case (_, exist) => !exist }

    if (nonExistsFields.nonEmpty) {
      new CheckResult(
        false,
        "field " + nonExistsFields
          .map { case (option) => "[" + option + "]" }
          .mkString(", ") + " not exist in table " + this.table)
    } else {
      val nonSupportedType = fields
        .map(field => (tableSchema(field), Clickhouse.supportOrNot(tableSchema(field))))
        .filter { case (_, exist) => !exist }
      if (nonSupportedType.nonEmpty) {
        new CheckResult(false,
          "clickHouse data type " + nonSupportedType
            .map { case (option) => "[" + option + "]" }
            .mkString(", ") + " not support in current version.")
      } else {
        new CheckResult(true, "")
      }
    }
  }

  private def renderDefaultStatement(index: Int, fieldType: String, statement: PreparedStatement): Unit = {
    fieldType match {
      case "DateTime" | "Date" | "String" =>
        statement.setString(index + 1, Clickhouse.renderStringDefault(fieldType))
      case "Int8" | "UInt8" | "Int16" | "Int32" | "UInt32" | "UInt16" =>
        statement.setInt(index + 1, 0)
      case "UInt64" | "Int64" =>
        statement.setLong(index + 1, 0)
      case "Float32" => statement.setFloat(index + 1, 0)
      case "Float64" => statement.setDouble(index + 1, 0)
      case Clickhouse.lowCardinalityPattern(lowCardinalityType) =>
        renderDefaultStatement(index, lowCardinalityType, statement)
      case Clickhouse.arrayPattern(_) => statement.setNull(index, java.sql.Types.ARRAY)
      case Clickhouse.nullablePattern(nullFieldType) => renderNullStatement(index, nullFieldType, statement)
      case _ => statement.setString(index + 1, "")
    }
  }

  private def renderNullStatement(index: Int, fieldType: String, statement: PreparedStatement): Unit = {
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

  private def renderBaseTypeStatement(index: Int,
                                      fieldIndex: Int,
                                      fieldType: String,
                                      item: Row,
                                      statement: PreparedStatement): Unit = {
    fieldType match {
      case "DateTime" | "Date" | "String" =>
        statement.setString(index + 1, item.getAs[String](fieldIndex))
      case "Int8" | "UInt8" | "Int16" | "UInt16" | "Int32" =>
        statement.setInt(index + 1, item.getAs[Int](fieldIndex))
      case "UInt32" | "UInt64" | "Int64" =>
        statement.setLong(index + 1, item.getAs[Long](fieldIndex))
      case "Float32" => statement.setFloat(index + 1, item.getAs[Float](fieldIndex))
      case "Float64" => statement.setDouble(index + 1, item.getAs[Double](fieldIndex))
      case Clickhouse.arrayPattern(_) =>
        statement.setArray(index + 1, item.getAs[java.sql.Array](fieldIndex))
      case "Decimal" => statement.setBigDecimal(index + 1, item.getAs[BigDecimal](fieldIndex))
      case _ => statement.setString(index + 1, item.getAs[String](fieldIndex))
    }
  }

  private def renderStatement(fields: util.List[String],
                              item: Row,
                              dsFields: Array[String],
                              statement: PreparedStatement): Unit = {
    for (i <- 0 until fields.size()) {
      val field = fields.get(i)
      val fieldType = tableSchema(field)
      if (dsFields.indexOf(field) == -1) {
        // specified field does not existed in row.
        renderDefaultStatement(i, fieldType, statement)
      } else {
        val fieldIndex = item.fieldIndex(field)
        if (item.isNullAt(fieldIndex)) {
          // specified field is Null in row.
          renderDefaultStatement(i, fieldType, statement)
        } else {
          fieldType match {
            case "String" | "DateTime" | "Date" | Clickhouse.arrayPattern(_) =>
              renderBaseTypeStatement(i, fieldIndex, fieldType, item, statement)
            case Clickhouse.floatPattern(_) | Clickhouse.intPattern(_) | Clickhouse.uintPattern(_) =>
              renderBaseTypeStatement(i, fieldIndex, fieldType, item, statement)
            case Clickhouse.nullablePattern(dataType) =>
              renderBaseTypeStatement(i, fieldIndex, dataType, item, statement)
            case Clickhouse.lowCardinalityPattern(dataType) =>
              renderBaseTypeStatement(i, fieldIndex, dataType, item, statement)
            case Clickhouse.decimalPattern(_) =>
              renderBaseTypeStatement(i, fieldIndex, "Decimal", item, statement)
            case _ => statement.setString(i + 1, item.getAs[String](field))
          }
        }
      }
    }
  }

  private def execute(statement: PreparedStatement, retry: Int): Unit = {
    val res = Try(statement.executeBatch())
    res match {
      case Success(_) => {
        statement.close()
      }
      case Failure(e: ClickHouseException) => {
        val errorCode = e.getErrorCode
        if (retryCodes.contains(errorCode)) {
          if (retry > 0) {
            execute(statement, retry - 1)
          } else {
            statement.close()
          }
        } else {
          throw e
        }
      }
      case Failure(e: ClickHouseUnknownException) => {
        statement.close()
        throw e
      }
      case Failure(e: Exception) => {
        throw e
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
    **/
  private[waterdrop] def supportOrNot(dataType: String): Boolean = {
    dataType match {
      case "Date" | "DateTime" | "String" =>
        true
      case nullablePattern(_) | floatPattern(_) | intPattern(_) | uintPattern(_) =>
        true
      case arrayPattern(_) =>
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
