package io.github.interestinglab.waterdrop.output

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}

import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

class Clickhouse (var config : Config) extends BaseOutput(config) {

  var schema: Map[String, String] = new HashMap[String, String]()
  var balanced: BalancedClickhouseDataSource = _
  var conn: ClickHouseConnectionImpl = _
  var initSQL: String = _
  var table: String = _
  var fields: java.util.List[String] = _


  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("host", "table", "database", "fields");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length > 0) {
      (false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    } else if (config.hasPath("username") && !config.hasPath("password") || config.hasPath("password") && !config.hasPath("username")) {
      (false, "please specify username and password at the same time")
    } else {

      val jdbcLink = String.format("jdbc:clickhouse://%s/%s", config.getString("host"),
        config.getString("database"))
      this.balanced = new BalancedClickhouseDataSource(jdbcLink)

      val conn = config.hasPath("username") match {
        case true => balanced.getConnection(config.getString("username"), config.getString("password")).asInstanceOf[ClickHouseConnectionImpl]
        case false => balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
      }

      this.table = config.getString("table")
      this.schema = getSchema(conn, table)


      this.fields = config.getStringList("fields")


      for (i <- 0 until fields.size()) {
        if (!this.schema.contains(fields.get(i))) {
          return (false, String.format("Table <%s> doesn't contain field <%s>", table, fields.get(i)))
        }
      }
      (true, "")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    this.initSQL = initPrepareSQL()
    logInfo(this.initSQL)
    super.prepare(spark, ssc)
  }

  override def process(df: DataFrame): Unit = {
    df.foreachPartition { iter =>

      val statement = this.conn.createClickHousePreparedStatement("")
      while (iter.hasNext) {
        val item = iter.next()
        println(item)
        println("hello world")
      }
    }
  }

  private def getSchema(conn: ClickHouseConnectionImpl, table: String) : Map[String, String] = {
    val sql = String.format("desc %s", table)
    val resultSet = conn.createStatement.executeQuery(sql)
    var schema = new HashMap[String, String]()
    while (resultSet.next()) {
      schema += (resultSet.getString(1) -> resultSet.getString(2))
    }
    schema
  }

  private def initPrepareSQL() : String = {
    val prepare =  List.fill(fields.size)("?")
    val sql = String.format("insert into %s (%s) values (%s)",
      this.table,
      this.fields.map(a => a)  .mkString(","),
      prepare.mkString(","))

    sql
  }
}


