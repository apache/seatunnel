package io.github.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}

import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

class Clickhouse extends BaseOutput {

  var schema: Map[String, String] = new HashMap[String, String]()
  var jdbcLink: String = _
  var initSQL: String = _
  var table: String = _
  var fields: java.util.List[String] = _

  var config: Config = ConfigFactory.empty()

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

    val requiredOptions = List("host", "table", "database", "fields");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length > 0) {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    } else if (config.hasPath("username") && !config.hasPath("password") || config.hasPath("password") && !config
        .hasPath("username")) {
      (false, "please specify username and password at the same time")
    } else {

      this.jdbcLink = String.format("jdbc:clickhouse://%s/%s", config.getString("host"), config.getString("database"))
      val balanced: BalancedClickhouseDataSource = new BalancedClickhouseDataSource(jdbcLink)

      val conn = config.hasPath("username") match {
        case true =>
          balanced
            .getConnection(config.getString("username"), config.getString("password"))
            .asInstanceOf[ClickHouseConnectionImpl]
        case false => balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
      }

      this.table = config.getString("table")
      this.schema = getSchema(conn, table)

      this.fields = config.getStringList("fields")

      val nonExistsFields = fields
        .map(field => (field, this.schema.contains(field)))
        .filter({ p =>
          val (field, exists) = p
          !exists
        })

      if (nonExistsFields.size > 0) {
        (
          false,
          "field " + nonExistsFields
            .map { option =>
              val (field, exists) = option
              "[" + field + "]"
            }
            .mkString(", ") + " not exist in table " + this.table)
      } else {
        (true, "")
      }
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    this.initSQL = initPrepareSQL()
    logInfo(this.initSQL)
    super.prepare(spark, ssc)
  }

  override def process(df: DataFrame): Unit = {
    df.foreachPartition { iter =>
      val executorBalanced = new BalancedClickhouseDataSource(this.jdbcLink)
      val executorConn = config.hasPath("username") match {
        case true =>
          executorBalanced
            .getConnection(config.getString("username"), config.getString("password"))
            .asInstanceOf[ClickHouseConnectionImpl]
        case false => executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
      }
      val statement = executorConn.createClickHousePreparedStatement(this.initSQL)

      while (iter.hasNext) {
        val item = iter.next()

        for (i <- 0 until fields.size()) {
          val field = fields.get(i)
          val fieldType = schema(field)
          fieldType match {
            case "DateTime" | "Date" | "String" => statement.setString(i + 1, item.getAs[String](field))
            case "Int8" | "Int16" | "Int32" | "UInt8" | "UInt16" => statement.setInt(i + 1, item.getAs[Int](field))
            case "UInt64" | "Int64" | "UInt32" => statement.setLong(i + 1, item.getAs[Long](field))
            case "Float32" | "Float64" => statement.setDouble(i + 1, item.getAs[Double](field))
            case _ => statement.setString(i + 1, item.getAs[String](field))
          }
        }

        statement.addBatch()
      }

      statement.executeBatch()
    }
  }

  private def getSchema(conn: ClickHouseConnectionImpl, table: String): Map[String, String] = {
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
}
