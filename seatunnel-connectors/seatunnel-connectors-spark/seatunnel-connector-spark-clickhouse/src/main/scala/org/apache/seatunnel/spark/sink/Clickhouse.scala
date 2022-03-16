/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.spark.sink

import net.jpountz.xxhash.{XXHash64, XXHashFactory}
import org.apache.commons.lang3.StringUtils
import java.math.{BigDecimal, BigInteger}
import java.sql.{Date, PreparedStatement, Timestamp}

import java.text.SimpleDateFormat
import java.util
import java.util.{Objects, Properties}
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.util.{Failure, Random, Success, Try}
import scala.util.matching.Regex
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.config.TypesafeConfigUtils.{extractSubConfig, hasSubConfig}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.sink.Clickhouse.{DistributedEngine, Shard}
import org.apache.spark.sql.{Dataset, Row}
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}
import ru.yandex.clickhouse.except.{ClickHouseException, ClickHouseUnknownException}

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import scala.annotation.tailrec

class Clickhouse extends SparkBatchSink {

  private var tableSchema: Map[String, String] = new HashMap[String, String]()
  private var initSQL: String = _
  private var table: String = _
  private var fields: java.util.List[String] = _
  private var retryCodes: java.util.List[Integer] = _
  private val shards = new util.TreeMap[Int, Shard]()
  private val clickhousePrefix = "clickhouse."
  private val properties: Properties = new Properties()

  // used for split mode
  private var splitMode: Boolean = false
  private val random = new Random()
  private var shardKey: String = ""
  private var shardKeyType: String = _
  private var shardTable: String = _
  private var shardWeightCount = 0
  // used for split mode end


  override def output(data: Dataset[Row], environment: SparkEnvironment): Unit = {
    val dfFields = data.schema.fieldNames
    val bulkSize = config.getInt("bulk_size")
    val retry = config.getInt("retry")

    if (!config.hasPath("fields")) {
      fields = dfFields.toList
      initSQL = initPrepareSQL()
    }
    data.foreachPartition { iter: Iterator[Row] =>

      val statementMap = this.shards.map(s => {
        val executorBalanced = new BalancedClickhouseDataSource(s._2.jdbc, this.properties)
        val executorConn = executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
        (s._2, executorConn.prepareStatement(this.initSQL))
      }).toMap

      // hashInstance cannot be serialized, can only be created in a partition
      val hashInstance = XXHashFactory.fastestInstance().hash64()

      val lengthMap = statementMap.map(s => (s._1, new AtomicLong(0)))
      for (item <- iter) {
        val shard = getRowShard(hashInstance, item)
        val statement = statementMap(shard)
        renderStatement(fields, item, dfFields, statement)
        statement.addBatch()
        val length = lengthMap(shard)
        if (length.addAndGet(1) >= bulkSize) {
          execute(statement, retry)
          length.set(0)
        }
      }
      statementMap.foreach(s => {
        execute(s._2, retry)
        s._2.close()
      })

    }
  }

  override def checkConfig(): CheckResult = {
    var checkResult = checkAllExists(config, "host", "table", "database", "username", "password")
    if (checkResult.isSuccess) {
      if (hasSubConfig(config, clickhousePrefix)) {
        extractSubConfig(config, clickhousePrefix, false).entrySet().foreach(e => {
          properties.put(e.getKey, String.valueOf(e.getValue.unwrapped()))
        })
      }

      properties.put("user", config.getString("username"))
      properties.put("password", config.getString("password"))

      if (config.hasPath("split_mode")) {
        splitMode = config.getBoolean("split_mode")
      }
      val database = config.getString("database")
      val host = config.getString("host")
      val globalJdbc = String.format("jdbc:clickhouse://%s/%s", host, database)
      val balanced: BalancedClickhouseDataSource =
        new BalancedClickhouseDataSource(globalJdbc, properties)
      val conn = balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
      val hostAndPort = host.split(":")
      this.table = config.getString("table")
      this.tableSchema = getClickHouseSchema(conn, this.table)
      if (splitMode) {
        val tableName = config.getString("table")
        val localTable = getClickHouseDistributedTable(conn, database, tableName)
        if (Objects.isNull(localTable)) {
          CheckResult.error(s"split mode only support table which engine is 'Distributed' at now")
        } else {
          this.shardTable = localTable.table
          val shardList = getClusterShardList(conn, localTable.clusterName, localTable.database, hostAndPort(1))
          var weight = 0
          for (elem <- shardList) {
            this.shards(weight) = elem
            weight += elem.shardWeight
          }
          this.shardWeightCount = weight
          if (config.hasPath("sharding_key") && StringUtils.isNotEmpty(config.getString("sharding_key"))) {
            this.shardKey = config.getString("sharding_key")
          }
        }
      } else {
        // only one connection
        this.shards(0) = new Shard(1, 1, 1, hostAndPort(0),
          hostAndPort(0), hostAndPort(1), database)
      }

      if (StringUtils.isNotEmpty(this.shardKey)) {
        if (!this.tableSchema.containsKey(this.shardKey)) {
          checkResult = CheckResult.error(
            s"not find field '${this.shardKey}' in table '${this.table}' as sharding key")
        } else {
          this.shardKeyType = this.tableSchema(this.shardKey)
        }
      }
      if (this.config.hasPath("fields")) {
        this.fields = config.getStringList("fields")
        checkResult = acceptedClickHouseSchema()
      }
    }
    checkResult
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
        "retry" -> 1))
    config = config.withFallback(defaultConfig)
    retryCodes = config.getIntList("retry_codes")
  }

  private def getClickHouseSchema(
                                   conn: ClickHouseConnectionImpl,
                                   table: String): Map[String, String] = {
    val sql = String.format("desc %s", table)
    val resultSet = conn.createStatement.executeQuery(sql)
    var schema = new HashMap[String, String]()
    while (resultSet.next()) {
      schema += (resultSet.getString(1) -> resultSet.getString(2))
    }
    schema
  }

  private def getClusterShardList(conn: ClickHouseConnectionImpl, clusterName: String,
                                  database: String, port: String): ListBuffer[Shard] = {
    val rs = conn.createStatement().executeQuery(
      s"select shard_num,shard_weight,replica_num,host_name,host_address," +
        s"port from system.clusters where cluster = '$clusterName'")
    // TODO The port will be used for data communication of the tcp protocol in the future
    // port is tcp protocol, need http protocol port at now
    val nodeList = mutable.ListBuffer[Shard]()
    while (rs.next()) {
      nodeList += new Shard(rs.getInt(1), rs.getInt(2),
        rs.getInt(3), rs.getString(4), rs.getString(5),
        port, database)
    }
    nodeList
  }

  private def getClickHouseDistributedTable(conn: ClickHouseConnectionImpl, database: String, table: String):
  DistributedEngine = {
    val rs = conn.createStatement().executeQuery(
      s"select engine_full from system.tables where " +
        s"database = '$database' and name = '$table' and engine = 'Distributed'")
    if (rs.next()) {
      // engineFull field will be like : Distributed(cluster, database, table[, sharding_key[, policy_name]])
      val engineFull = rs.getString(1)
      val infos = engineFull.substring(12).split(",").map(s => s.replaceAll("'", ""))
      new DistributedEngine(infos(0), infos(1).trim, infos(2).replaceAll("\\)", "").trim)
    } else {
      null
    }
  }

  private def getRowShard(hashInstance: XXHash64, row: Row): Shard = {
    if (splitMode) {
      if (StringUtils.isEmpty(this.shardKey) || row.schema.fieldNames.indexOf(this.shardKey) == -1) {
        this.shards.lowerEntry(this.random.nextInt(this.shardWeightCount) + 1).getValue
      } else {
        val fieldIndex = row.fieldIndex(this.shardKey)
        if (row.isNullAt(fieldIndex)) {
          this.shards.lowerEntry(this.random.nextInt(this.shardWeightCount) + 1).getValue
        } else {
          var offset = 0
          this.shardKeyType match {
            case Clickhouse.floatPattern(_) =>
              offset = row.getFloat(fieldIndex).toInt % this.shardWeightCount
            case Clickhouse.intPattern(_) | Clickhouse.uintPattern(_) =>
              offset = row.getInt(fieldIndex) % this.shardWeightCount
            case Clickhouse.decimalPattern(_) =>
              offset = row.getDecimal(fieldIndex).toBigInteger.mod(BigInteger.valueOf(this
                .shardWeightCount)).intValue()
            case _ =>
              offset = (hashInstance.hash(ByteBuffer.wrap(row.getString(fieldIndex).getBytes), 0) & Long.MaxValue %
                this.shardWeightCount).toInt
          }
          this.shards.lowerEntry(offset + 1).getValue
        }
      }
    } else {
      this.shards.head._2
    }
  }

  private def initPrepareSQL(): String = {

    val prepare = List.fill(fields.size)("?")
    var table = this.table
    if (splitMode) {
      table = this.shardTable
    }
    val sql = String.format(
      "insert into %s (%s) values (%s)",
      table,
      this.fields.map(a => a).mkString(","),
      prepare.mkString(","))

    sql
  }

  private def acceptedClickHouseSchema(): CheckResult = {
    val nonExistsFields = fields
      .map(field => (field, tableSchema.contains(field)))
      .filter { case (_, exist) => !exist }

    if (nonExistsFields.nonEmpty) {
      CheckResult.error(
        "field " + nonExistsFields
          .map(option => "[" + option + "]")
          .mkString(", ") + " not exist in table " + this.table)
    } else {
      val nonSupportedType = fields
        .map(field => (tableSchema(field), Clickhouse.supportOrNot(tableSchema(field))))
        .filter { case (_, exist) => !exist }
      if (nonSupportedType.nonEmpty) {
        CheckResult.error(
          "clickHouse data type " + nonSupportedType
            .map(option => "[" + option + "]")
            .mkString(", ") + " not support in current version.")
      } else {
        CheckResult.success()
      }
    }
  }

  @tailrec
  private def renderDefaultStatement(
                                      index: Int,
                                      fieldType: String,
                                      statement: PreparedStatement): Unit = {
    fieldType match {
      case "DateTime" | "Date" | "String" =>
        statement.setString(index + 1, Clickhouse.renderStringDefault(fieldType))
      case Clickhouse.datetime64Pattern(_) =>
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
      case Clickhouse.nullablePattern(nullFieldType) =>
        renderNullStatement(index, nullFieldType, statement)
      case _ => statement.setString(index + 1, "")
    }
  }

  private def renderNullStatement(
                                   index: Int,
                                   fieldType: String,
                                   statement: PreparedStatement): Unit = {
    fieldType match {
      case "String" =>
        statement.setNull(index + 1, java.sql.Types.VARCHAR)
      case "DateTime" => statement.setNull(index + 1, java.sql.Types.DATE)
      case Clickhouse.datetime64Pattern(_) => statement.setNull(index + 1, java.sql.Types.TIMESTAMP)
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
                                       fieldIndex: Int,
                                       fieldType: String,
                                       item: Row,
                                       statement: PreparedStatement): Unit = {
    fieldType match {
      case "String" =>
        statement.setString(index + 1, item.getAs[String](fieldIndex))
      case "Date" =>
        statement.setDate(index + 1, item.getAs[Date](fieldIndex))
      case "DateTime" | Clickhouse.datetime64Pattern(_) =>
        statement.setTimestamp(index + 1, item.getAs[Timestamp](fieldIndex))
      case "Int8" | "UInt8" | "Int16" | "UInt16" | "Int32" =>
        statement.setInt(index + 1, item.getAs[Int](fieldIndex))
      case "UInt32" | "UInt64" | "Int64" =>
        statement.setLong(index + 1, item.getAs[Long](fieldIndex))
      case "Float32" =>
        val value = item.get(fieldIndex)
        value match {
          case decimal: BigDecimal =>
            statement.setFloat(index + 1, decimal.floatValue())
          case _ =>
            statement.setFloat(index + 1, value.asInstanceOf[Float])
        }
      case "Float64" =>
        val value = item.get(fieldIndex)
        value match {
          case decimal: BigDecimal =>
            statement.setDouble(index + 1, decimal.doubleValue())
          case _ =>
            statement.setDouble(index + 1, value.asInstanceOf[Double])
        }
      case Clickhouse.arrayPattern(_) =>
        statement.setArray(index + 1, item.getAs[java.sql.Array](fieldIndex))
      case "Decimal" => statement.setBigDecimal(index + 1, item.getAs[BigDecimal](fieldIndex))
      case _ => statement.setString(index + 1, item.getAs[String](fieldIndex))
    }
  }

  private def renderStatement(
                               fields: util.List[String],
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
            case "String" | "DateTime" | Clickhouse.datetime64Pattern(_) | "Date" | Clickhouse.arrayPattern(_) =>
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

  @tailrec
  private def execute(statement: PreparedStatement, retry: Int): Unit = {
    val res = Try(statement.executeBatch())
    res match {
      case Success(_) =>
      case Failure(e: ClickHouseException) =>
        val errorCode = e.getErrorCode
        if (retryCodes.contains(errorCode)) {
          if (retry > 0) {
            execute(statement, retry - 1)
          } else {
            statement.close()
            throw e
          }
        } else {
          throw e
        }
      case Failure(e: ClickHouseUnknownException) =>
        statement.close()
        throw e
      case Failure(e: Exception) =>
        throw e
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
  val datetime64Pattern: Regex = "(DateTime64\\(.*\\))".r

  /**
   * Seatunnel support this clickhouse data type or not.
   *
   * @param dataType ClickHouse Data Type
   * @return Boolean
   */
  private[seatunnel] def supportOrNot(dataType: String): Boolean = {
    dataType match {
      case "Date" | "DateTime" | "String" =>
        true
      case nullablePattern(_) | floatPattern(_) | intPattern(_) | uintPattern(_) | datetime64Pattern(_) =>
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

  private[seatunnel] def renderStringDefault(fieldType: String): String = {
    fieldType match {
      case "DateTime" =>
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        dateFormat.format(System.currentTimeMillis())
      case datetime64Pattern(_) =>
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        dateFormat.format(System.currentTimeMillis())
      case "Date" =>
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        dateFormat.format(System.currentTimeMillis())
      case "String" =>
        ""
    }
  }

  class DistributedEngine(val clusterName: String, val database: String,
                          val table: String) {
  }

  class Shard(val shardNum: Int, val shardWeight: Int, val replicaNum: Int,
              val hostname: String, val hostAddress: String, val port: String,
              val database: String) extends Serializable {
    val jdbc = s"jdbc:clickhouse://$hostAddress:$port/$database"
  }
}
