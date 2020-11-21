package io.github.interestinglab.waterdrop.output.utils

import java.math.BigDecimal
import java.sql.{Array, DriverManager, ResultSet}

import io.github.interestinglab.waterdrop.output.batch.Clickhouse
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import ru.yandex.clickhouse.except.{ClickHouseException, ClickHouseUnknownException}
import ru.yandex.clickhouse.{ClickHouseConnectionImpl, ClickHousePreparedStatement}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Failure, Random, Success, Try}

case class ClickhouseUtilParam(clusterInfo: ArrayBuffer[(String, Int, Int, String, Int)], database: String, user: String, password: String, initSql: String, tableSchema: Map[String, String], fields: List[String], shardingKey: String, batchSize: Int, retry: Int,retryCodes: List[Integer])


class ClickhouseUtil(utilParam: ClickhouseUtilParam) extends Serializable with Logging {

  private val connectionList = new ListBuffer[ClickHouseConnectionImpl]
  private val preparedStatementList = new ListBuffer[ClickHousePreparedStatement]
  private val rand = "rand()"

  def initConnectionList(): Unit = {
    if (utilParam.clusterInfo == null || utilParam.clusterInfo.isEmpty) throw new Exception("ClickHouse connection info is empty!")
    for (elem <- utilParam.clusterInfo) {
      val host: String = elem._4
      val port: Int = elem._5
      val connectionHost = s"jdbc:clickhouse://$host:$port/${utilParam.database}"
      logInfo(s"will connection to ${connectionHost}, user is ${utilParam.user}")
      val connection: ClickHouseConnectionImpl = DriverManager.getConnection(connectionHost, utilParam.user, utilParam.password).asInstanceOf[ClickHouseConnectionImpl]
      val statement: ClickHousePreparedStatement = connection.createClickHousePreparedStatement(utilParam.initSql, ResultSet.TYPE_FORWARD_ONLY)
      preparedStatementList.append(statement)
      connectionList.append(connection)
    }
  }

  def closeConnection(): Unit = {
    logInfo("will close all connection")
    for (elem <- preparedStatementList) {
      elem.close()
    }
    for (elem <- connectionList) {
      elem.close()
    }
    logInfo("all connection closed!")
  }

  /**
   * Add data, batch send
   * if table is distributed, use sharding key get the value, choose the insert node. use the same connection for a batch of data.
   * if the table is not a distributed table, randomly select a node.
   *
   * @param rows
   */
  def add(rows: Iterator[Row]): Unit = {
    rows.grouped(utilParam.batchSize).foreach(batchData => {
      var preparedStatement: ClickHousePreparedStatement = null
      // each batch use same PreparedStatement
      batchData.foreach(row => {
        if (preparedStatement == null) {
          if (utilParam.shardingKey == null) {
            // for stand alone case, randomly select a node
            val randomIndex: Int = Random.nextInt(connectionList.length)
            preparedStatement = preparedStatementList(randomIndex)
          } else {
            if (utilParam.shardingKey == rand) {
              val index: Int = Random.nextInt(connectionList.size)
              preparedStatement = preparedStatementList(index)
            } else {
              // for distributed table case. Use shard key to select insert node
              // shardingKey is a numeric type, use Long
              val shardingNumeric: AnyVal = row.getAs(utilParam.shardingKey)
              var shardingValue: Long = 0L
              shardingNumeric match {
                case v: Int =>
                  shardingValue = v.asInstanceOf[Long]
                case v: Long =>
                  shardingValue = v
                case _ =>
                  throw new Exception("sharding key is not an Numeric!")
              }
              val slot: Int = (shardingValue % connectionList.size).intValue()
              preparedStatement = preparedStatementList(slot)
            }
          }
        }

        renderStatement(utilParam.fields, row, row.schema.fieldNames, preparedStatement)
        preparedStatement.addBatch()
      })
      execute(preparedStatement,utilParam.retry)
    })
  }

  private def execute(statement: ClickHousePreparedStatement, retry: Int): Unit = {
    val res = Try(statement.executeBatch())
    res match {
      case Success(_) => {
        logInfo("Insert into ClickHouse succeed")
      }
      case Failure(e: ClickHouseException) => {
        val errorCode = e.getErrorCode
        if (utilParam.retryCodes.contains(errorCode)) {
          logError("Insert into ClickHouse failed. Reason: ", e)
          if (retry > 0) {
            execute(statement, retry - 1)
          } else {
            logError("Insert into ClickHouse failed and retry failed, drop this bulk.")
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

  private def renderStatement(fields: List[String], item: Row, dsFields: scala.Array[String], statement: ClickHousePreparedStatement): Unit = {
    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = utilParam.tableSchema(field)
      if (dsFields.indexOf(field) == -1) {
        // specified field does not existed in row.
        renderDefaultStatement(i, fieldType, statement)
      } else {
        val fieldIndex = item.fieldIndex(field)
        if (item.isNullAt(fieldIndex)) {
          // specified field is Null in Row.
          renderDefaultStatement(i, fieldType, statement)
        } else {
          renderStatementEntry(i, fieldIndex, fieldType, item, statement)
        }
      }
    }
  }

  private def renderBaseTypeStatement(index: Int, fieldIndex: Int, fieldType: String, item: Row, statement: ClickHousePreparedStatement): Unit = {
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
        statement.setArray(index + 1, item(index).asInstanceOf[Array])
      case "Decimal" => statement.setBigDecimal(index + 1, item.getAs[BigDecimal](fieldIndex))
      case _ => statement.setString(index + 1, item.getAs[String](fieldIndex))
    }
  }

  private def renderStatementEntry(index: Int, fieldIndex: Int, fieldType: String, item: Row, statement: ClickHousePreparedStatement): Unit = {
    fieldType match {
      case "String" | "DateTime" | "Date" | Clickhouse.arrayPattern(_) =>
        renderBaseTypeStatement(index, fieldIndex, fieldType, item, statement)
      case Clickhouse.floatPattern(_) | Clickhouse.intPattern(_) | Clickhouse.uintPattern(_) =>
        renderBaseTypeStatement(index, fieldIndex, fieldType, item, statement)
      case Clickhouse.nullablePattern(dataType) =>
        renderStatementEntry(index, fieldIndex, dataType, item, statement)
      case Clickhouse.lowCardinalityPattern(dataType) =>
        renderBaseTypeStatement(index, fieldIndex, dataType, item, statement)
      case Clickhouse.decimalPattern(_) =>
        renderBaseTypeStatement(index, fieldIndex, "Decimal", item, statement)
      case _ => statement.setString(index + 1, item.getAs[String](fieldIndex))
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
      case Clickhouse.lowCardinalityPattern(lowCardinalityType) =>
        renderDefaultStatement(index, lowCardinalityType, statement)
      case Clickhouse.arrayPattern(_) => statement.setArray(index + 1, List().asInstanceOf[Array])
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
      case "Array" => statement.setNull(index + 1, java.sql.Types.ARRAY)
      case Clickhouse.decimalPattern(_) => statement.setNull(index + 1, java.sql.Types.DECIMAL)
    }
  }


}
