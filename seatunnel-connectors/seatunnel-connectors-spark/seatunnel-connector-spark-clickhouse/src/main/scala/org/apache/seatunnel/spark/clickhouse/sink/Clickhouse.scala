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
package org.apache.seatunnel.spark.clickhouse.sink

import net.jpountz.xxhash.{XXHash64, XXHashFactory}
import org.apache.commons.lang3.StringUtils

import java.math.{BigDecimal, BigInteger}
import java.sql.{Date, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util
import java.util.{Objects, Properties}
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.config.TypesafeConfigUtils.{extractSubConfig, hasSubConfig}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.clickhouse.Config.{BULK_SIZE, DATABASE, FIELDS, HOST, PASSWORD, RETRY, RETRY_CODES, SHARDING_KEY, SPLIT_MODE, TABLE, USERNAME}
import org.apache.seatunnel.spark.clickhouse.sink.Clickhouse.{Shard, acceptedClickHouseSchema, distributedEngine, getClickHouseDistributedTable, getClickHouseSchema, getClickhouseConnection, getClusterShardList, getDefaultValue, getRowShard, parseHost}
import org.apache.spark.sql.{Dataset, Row}
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseArray, ClickHouseConnectionImpl, ClickHousePreparedStatementImpl}
import ru.yandex.clickhouse.except.ClickHouseException
import ru.yandex.clickhouse.domain.ClickHouseDataType
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
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
  private var multiHosts: String = _

  // used for split mode
  private var splitMode: Boolean = false
  private val random = ThreadLocalRandom.current()
  private var shardKey: String = ""
  private var shardKeyType: String = _
  private var shardTable: String = _
  private var shardWeightCount = 0
  // used for split mode end


  override def output(data: Dataset[Row], environment: SparkEnvironment): Unit = {
    val dfFields = data.schema.fieldNames
    val bulkSize = config.getInt(BULK_SIZE)
    val retry = config.getInt(RETRY)

    if (!config.hasPath(FIELDS)) {
      fields = dfFields.toList
      initSQL = initPrepareSQL()
    }
    data.foreachPartition { iter: Iterator[Row] =>

      val statementMap = this.shards.map(s => {
        // if use splitMode, jdbcUrl should use the shard itself url, or else should use multiHosts
        var jdbcUrl = String.format("jdbc:clickhouse://%s/%s", multiHosts, s._2.database)
        if (splitMode) {
          jdbcUrl = s._2.jdbc
        }
        val executorBalanced = new BalancedClickhouseDataSource(jdbcUrl, this.properties)
        val executorConn = executorBalanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
        (s._2, executorConn.prepareStatement(this.initSQL).asInstanceOf[ClickHousePreparedStatementImpl])
      }).toMap

      // hashInstance cannot be serialized, can only be created in a partition
      val hashInstance = XXHashFactory.fastestInstance().hash64()

      val lengthMap = statementMap.map(s => (s._1, new AtomicLong(0)))
      for (item <- iter) {
        val shard = getRowShard(this.splitMode, this.shards, this.shardKey, this.shardKeyType, this
          .shardWeightCount, this.random, hashInstance, item)
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
    var checkResult = checkAllExists(config, HOST, TABLE, DATABASE, USERNAME, PASSWORD)
    if (checkResult.isSuccess) {
      if (hasSubConfig(config, clickhousePrefix)) {
        extractSubConfig(config, clickhousePrefix, false).entrySet().foreach(e => {
          properties.put(e.getKey, String.valueOf(e.getValue.unwrapped()))
        })
      }

      properties.put("user", config.getString(USERNAME))
      properties.put("password", config.getString(PASSWORD))

      if (config.hasPath(SPLIT_MODE)) {
        splitMode = config.getBoolean(SPLIT_MODE)
      }
      val database = config.getString(DATABASE)
      val hosts = parseHost(config.getString(HOST))
      multiHosts = hosts.map(_.hostAndPort).mkString(",")
      val conn = getClickhouseConnection(multiHosts, database, properties)
      this.table = config.getString(TABLE)
      this.tableSchema = getClickHouseSchema(conn, this.table).toMap
      if (splitMode) {
        val tableName = config.getString(TABLE)
        val localTable = getClickHouseDistributedTable(conn, database, tableName)
        if (Objects.isNull(localTable)) {
          CheckResult.error(s"split mode only support table which engine is '$distributedEngine' at now")
        } else {
          this.shardTable = localTable.table
          val shardList = getClusterShardList(conn, localTable.clusterName, localTable.database, hosts)
          var weight = 0
          for (elem <- shardList) {
            this.shards(weight) = elem
            weight += elem.shardWeight
          }
          this.shardWeightCount = weight
          if (config.hasPath(SHARDING_KEY) && StringUtils.isNotEmpty(config.getString(SHARDING_KEY))) {
            this.shardKey = config.getString(SHARDING_KEY)
          }
        }
      } else {
        // only one connection, just use the first host in hosts, as it will be ignored when output data
        this.shards(0) = Shard(1, 1, 1, hosts.head.host, hosts.head.host, hosts.head.port, database)
      }

      if (StringUtils.isNotEmpty(this.shardKey)) {
        if (!this.tableSchema.containsKey(this.shardKey)) {
          checkResult = CheckResult.error(
            s"not find field '${this.shardKey}' in table '${this.table}' as sharding key")
        } else {
          this.shardKeyType = this.tableSchema(this.shardKey)
        }
      }
      if (this.config.hasPath(FIELDS)) {
        this.fields = config.getStringList(FIELDS)
        checkResult = acceptedClickHouseSchema(this.fields.toList, this.tableSchema, this.table)
      }
    }
    checkResult
  }

  override def prepare(env: SparkEnvironment): Unit = {
    if (config.hasPath(FIELDS)) {
      this.initSQL = initPrepareSQL()
    }

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        BULK_SIZE -> 20000,
        // "retry_codes" -> util.Arrays.asList(ClickHouseErrorCode.NETWORK_ERROR.code),
        RETRY_CODES -> util.Arrays.asList(),
        RETRY -> 1))
    config = config.withFallback(defaultConfig)
    retryCodes = config.getIntList(RETRY_CODES)
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

  private def renderDefaultStatement(
                                      index: Int,
                                      fieldType: String,
                                      statement: ClickHousePreparedStatementImpl): Unit = {
    statement.setObject(index + 1, getDefaultValue(fieldType))
  }

  private def renderBaseTypeStatement(
                                       index: Int,
                                       fieldIndex: Int,
                                       fieldType: String,
                                       item: Row,
                                       statement: ClickHousePreparedStatementImpl): Unit = {
    fieldType match {
      case "String" =>
        statement.setString(index + 1, item.getAs[String](fieldIndex))
      case "Date" =>
        val value = item.get(fieldIndex)
        value match {
          case date: Date =>
            statement.setDate(index + 1, date)
          case _ =>
            statement.setDate(index + 1, Date.valueOf(value.toString))
        }
      case "DateTime" | Clickhouse.datetime64Pattern(_) =>
        val value = item.get(fieldIndex)
        value match {
          case timestamp: Timestamp =>
            statement.setTimestamp(index + 1, timestamp)
          case _ =>
            statement.setTimestamp(index + 1, Timestamp.valueOf(value.toString))
        }
      case "Int8" | "UInt8" | "Int16" | "UInt16" | "Int32" =>
        val value = item.get(fieldIndex)
        value match {
          case byte: Byte =>
            statement.setByte(index + 1, byte.byteValue())
          case short: Short =>
            statement.setShort(index + 1, short.shortValue())
          case _ =>
            statement.setInt(index + 1, value.asInstanceOf[Int])
        }
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
        val value = item.getAs[Seq[Any]](fieldIndex).toArray
        statement.setArray(index + 1, new ClickHouseArray(ClickHouseDataType.String, value))
      case Clickhouse.decimalPattern(_) => statement.setBigDecimal(index + 1, item.getAs[BigDecimal](fieldIndex))
      case _ => statement.setString(index + 1, item.getAs[String](fieldIndex))
    }
  }

  private def renderStatement(
                               fields: util.List[String],
                               item: Row,
                               dsFields: Array[String],
                               statement: ClickHousePreparedStatementImpl): Unit = {
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
      case Failure(e: Exception) =>
        statement.close()
        throw e
    }
  }

  override def getPluginName: String = "Clickhouse"
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
  val distributedEngine = "Distributed"

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

  def parseHost(host: String): List[HostAndPort] = {
    host.split(",")
      .map(_.trim)
      .map(_.split(":"))
      .map(hostAndPort => HostAndPort(hostAndPort(0), hostAndPort(1)))
      .toList
  }

  def getClusterShardList(conn: ClickHouseConnectionImpl, clusterName: String,
                          database: String, hosts: List[HostAndPort]): ListBuffer[Shard] = {
    val rs = conn.createStatement().executeQuery(
      s"select shard_num,shard_weight,replica_num,host_name,host_address," +
        s"port from system.clusters where cluster = '$clusterName'")
    // TODO The port will be used for data communication of the tcp protocol in the future
    // port is tcp protocol, need http protocol port at now
    val nodeList = mutable.ListBuffer[Shard]()
    val defaultPort = hosts.head.port
    while (rs.next()) {
      val hostname = rs.getString(4)
      val hostAddress = rs.getString(5)
      val port = hosts.toStream
        .filter(hostAndPort => hostname.equals(hostAndPort.host) || hostAddress.equals(hostAndPort.host))
        .map(_.port)
        .headOption
        .getOrElse(defaultPort)
      nodeList += Shard(rs.getInt(1), rs.getInt(2),
        rs.getInt(3), hostname, hostAddress, port, database)
    }
    nodeList
  }

  def getClickHouseDistributedTable(conn: ClickHouseConnectionImpl, database: String, table: String):
  DistributedEngine = {
    val rs = conn.createStatement().executeQuery(
      s"select engine_full from system.tables where " +
        s"database = '$database' and name = '$table' and engine = '$distributedEngine'")
    if (rs.next()) {
      // engineFull field will be like : Distributed(cluster, database, table[, sharding_key[, policy_name]])
      val engineFull = rs.getString(1)
      val infos = engineFull.substring(12).split(",").map(s => s.replaceAll("'", ""))
      DistributedEngine(infos(0), infos(1).trim, infos(2).replaceAll("\\)", "").trim)
    } else {
      null
    }
  }

  def getRowShard(splitMode: Boolean, shards: util.TreeMap[Int, Shard], shardKey: String, shardKeyType: String,
                  shardWeightCount: Int, random: ThreadLocalRandom, hashInstance: XXHash64,
                  row: Row): Shard = {
    if (splitMode) {
      if (StringUtils.isEmpty(shardKey) || row.schema.fieldNames.indexOf(shardKey) == -1) {
        shards.lowerEntry(random.nextInt(shardWeightCount) + 1).getValue
      } else {
        val fieldIndex = row.fieldIndex(shardKey)
        if (row.isNullAt(fieldIndex)) {
          shards.lowerEntry(random.nextInt(shardWeightCount) + 1).getValue
        } else {
          var offset = 0
          shardKeyType match {
            case Clickhouse.floatPattern(_) =>
              offset = row.getFloat(fieldIndex).toInt % shardWeightCount
            case Clickhouse.intPattern(_) | Clickhouse.uintPattern(_) =>
              offset = row.getInt(fieldIndex) % shardWeightCount
            case Clickhouse.decimalPattern(_) =>
              offset = row.getDecimal(fieldIndex).toBigInteger.mod(BigInteger.valueOf(shardWeightCount)).intValue()
            case _ =>
              offset = (hashInstance.hash(ByteBuffer.wrap(row.getString(fieldIndex).getBytes), 0) & Long.MaxValue %
                shardWeightCount).toInt
          }
          shards.lowerEntry(offset + 1).getValue
        }
      }
    } else {
      shards.head._2
    }
  }


  def acceptedClickHouseSchema(fields: List[String], tableSchema: Map[String, String], table: String)
  : CheckResult = {
    val nonExistsFields = fields
      .map(field => (field, tableSchema.contains(field)))
      .filter { case (_, exist) => !exist }

    if (nonExistsFields.nonEmpty) {
      CheckResult.error(
        "field " + nonExistsFields
          .map(option => "[" + option + "]")
          .mkString(", ") + " not exist in table " + table)
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

  def getClickHouseSchema(
                           conn: ClickHouseConnectionImpl,
                           table: String): util.LinkedHashMap[String, String] = {
    val sql = String.format("desc %s", table)
    val resultSet = conn.createStatement.executeQuery(sql)
    val schema = new util.LinkedHashMap[String, String]()
    while (resultSet.next()) {
      schema.put(resultSet.getString(1), resultSet.getString(2))
    }
    schema
  }

  @tailrec
  def getDefaultValue(fieldType: String): Object = {
    fieldType match {
      case "DateTime" | "Date" | "String" =>
        Clickhouse.renderStringDefault(fieldType)
      case Clickhouse.datetime64Pattern(_) =>
        Clickhouse.renderStringDefault(fieldType)
      case Clickhouse.datetime64Pattern(_) =>
        Clickhouse.renderStringDefault(fieldType)
      case "Int8" | "UInt8" | "Int16" | "Int32" | "UInt32" | "UInt16" | "UInt64" |
           "Int64" | "Float32" | "Float64" =>
        new Integer(0)
      case Clickhouse.lowCardinalityPattern(lowCardinalityType) =>
        getDefaultValue(lowCardinalityType)
      case Clickhouse.arrayPattern(_) | Clickhouse.nullablePattern(_) =>
        null
      case _ => ""
    }
  }


  def getClickhouseConnection(host: String, database: String, properties: Properties): ClickHouseConnectionImpl = {
    val globalJdbc = String.format("jdbc:clickhouse://%s/%s", host, database)
    val balanced: BalancedClickhouseDataSource = new BalancedClickhouseDataSource(globalJdbc, properties)
    balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
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

  case class DistributedEngine(clusterName: String, database: String, table: String) {
  }

  case class Shard(shardNum: Int, shardWeight: Int, replicaNum: Int,
                   hostname: String, hostAddress: String, port: String,
                   database: String) extends Serializable {
    val jdbc = s"jdbc:clickhouse://$hostAddress:$port/$database"
  }

  case class HostAndPort(host: String, port: String) {
    val hostAndPort = s"$host:$port"
  }
}
