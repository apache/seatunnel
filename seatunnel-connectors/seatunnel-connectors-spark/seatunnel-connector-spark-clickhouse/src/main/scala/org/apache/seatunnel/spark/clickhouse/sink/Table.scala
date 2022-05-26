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

import org.apache.commons.lang3.StringUtils
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.clickhouse.sink.Clickhouse.{HostAndPort, Shard, distributedEngine, getClickHouseDistributedTable, getClickHouseSchema, getClickhouseConnection, getClusterShardList, parseHost}
import org.apache.seatunnel.spark.clickhouse.sink.ClickhouseFile.getClickhouseTableInfo
import ru.yandex.clickhouse.ClickHouseConnectionImpl

import java.util
import java.util.Properties
import scala.collection.JavaConversions

class Table(val name: String, val database: String, val engine: String, val createTableDDL: String,
            val engineFull: String, val dataPaths: List[String]) extends Serializable {

  var shards = new util.TreeMap[Int, Shard]()
  private var localTable: String = _
  var shardWeightCount: Int = 0
  var shardKey: String = _
  private var localDataPaths: Map[Shard, List[String]] = _
  var tableSchema: util.LinkedHashMap[String, String] = new util.LinkedHashMap[String, String]()
  var shardKeyType: String = _
  var localCreateTableDDL: String = createTableDDL
  var localTableEngine: String = _

  def initTableInfo(hosts: List[HostAndPort], conn: ClickHouseConnectionImpl): Unit = {
    if (shards.size() == 0) {
      if (distributedEngine.equals(this.engine)) {
        val localTable = getClickHouseDistributedTable(conn, database, name)
        this.localTable = localTable.table
        val shardList = getClusterShardList(conn, localTable.clusterName, localTable.database, hosts)
        var weight = 0
        for (elem <- shardList) {
          this.shards.put(weight, elem)
          weight += elem.shardWeight
        }
        this.shardWeightCount = weight
        val localTableInfo = getClickhouseTableInfo(conn, localTable.database, localTable.table)._2
        this.localTableEngine = localTableInfo.engine
        this.localCreateTableDDL = localizationEngine(this.localTableEngine, localTableInfo.createTableDDL)
      } else {
        this.shards.put(0, Shard(1, 1, 1, hosts.head.host, hosts.head.host, hosts.head.port, database))
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

  /**
   * Localization the engine in clickhouse local table's createTableDDL to support specific engine.
   * For example: change ReplicatedMergeTree to MergeTree.
   * @param engine original engine of clickhouse local table
   * @param ddl createTableDDL of clickhouse local table
   * @return createTableDDL of clickhouse local table which can support specific engine
   * TODO: support more engine
   */
  def localizationEngine(engine: String, ddl: String): String = {
    if ("ReplicatedMergeTree".equalsIgnoreCase(engine)) {
      ddl.replaceAll("""ReplicatedMergeTree(\([^\)]*\))""", "MergeTree()")
    } else ddl
  }

}
