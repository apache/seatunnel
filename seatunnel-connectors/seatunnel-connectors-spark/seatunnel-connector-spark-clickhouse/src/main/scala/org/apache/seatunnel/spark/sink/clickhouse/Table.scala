package org.apache.seatunnel.spark.sink.clickhouse

import org.apache.commons.lang3.StringUtils
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.sink.Clickhouse.{Shard, distributedEngine, getClickHouseDistributedTable, getClickHouseSchema, getClickhouseConnection, getClusterShardList}
import org.apache.seatunnel.spark.sink.ClickhouseFile.getClickhouseTableInfo
import ru.yandex.clickhouse.ClickHouseConnectionImpl

import java.util
import java.util.Properties
import scala.collection.JavaConversions

class Table(val name: String, val database: String, val engine: String, val createTableDDL: String, val
engineFull: String, val dataPaths: List[String]) extends Serializable {
  // TODO 不同的表有不同的dataPaths
  var shards = new util.TreeMap[Int, Shard]()
  private var localTable: String = _
  var shardWeightCount: Int = 0
  var shardKey: String = _
  private var localDataPaths: Map[Shard, List[String]] = _
  var tableSchema: util.LinkedHashMap[String, String] = new util.LinkedHashMap[String, String]()
  var shardKeyType: String = _
  var localCreateTableDDL: String = createTableDDL

  def initTableInfo(host: String, conn: ClickHouseConnectionImpl): Unit = {
    if (shards.size() == 0) {
      val hostAndPort = host.split(":")
      if (distributedEngine.equals(this.engine)) {
        val localTable = getClickHouseDistributedTable(conn, database, name)
        this.localTable = localTable.table
        val shardList = getClusterShardList(conn, localTable.clusterName, localTable.database, hostAndPort(1))
        var weight = 0
        for (elem <- shardList) {
          this.shards.put(weight, elem)
          weight += elem.shardWeight
        }
        this.shardWeightCount = weight
        this.localCreateTableDDL = getClickhouseTableInfo(conn, localTable.database, localTable.table)._2.createTableDDL
      } else {
        this.shards.put(0, Shard(1, 1, 1, hostAndPort(0), hostAndPort(0), hostAndPort(1), database))
      }
    }
  }

  def getLocalTableName: String = {
    if (this.engine.equals(distributedEngine)) {
      localTable
    } else {
      name
    }
  }

  def getLocalDataPath(shard: Shard): List[String] = {
    if (!this.engine.equals(distributedEngine)) {
      dataPaths
    } else {
      localDataPaths(shard)
    }
  }

  def initShardDataPath(username: String, password: String): Unit = {
    val properties: Properties = new Properties()
    properties.put("user", username)
    properties.put("password", password)
    this.localDataPaths = JavaConversions.collectionAsScalaIterable(this.shards.values).map(s => {
      val conn = getClickhouseConnection(s.hostAddress + ":" + s.port, s.database, properties)
      (s, getClickhouseTableInfo(conn, s.database, getLocalTableName)._2.dataPaths)
    }).toMap
  }

  def getCreateDDLNoDatabase: String = {
    this.localCreateTableDDL.replace(this.database + ".", "")
  }

  def prepareShardInfo(conn: ClickHouseConnectionImpl): CheckResult = {
    this.tableSchema = getClickHouseSchema(conn, name)
    if (StringUtils.isNotEmpty(this.shardKey)) {
      if (!this.tableSchema.containsKey(this.shardKey)) {
        CheckResult.error(
          s"not find field '${this.shardKey}' in table '${this.name}' as sharding key")
      } else {
        this.shardKeyType = this.tableSchema.get(this.shardKey)
        CheckResult.success()
      }
    } else {
      CheckResult.success()
    }
  }
}
