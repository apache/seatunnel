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

import com.jcraft.jsch.JSch
import net.jpountz.xxhash.XXHashFactory
import org.apache.commons.lang3.StringUtils
import org.apache.ivy.plugins.repository.ssh.Scp
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.sink.Clickhouse._
import org.apache.seatunnel.spark.sink.ClickhouseFile.{CLICKHOUSE_FILE_PREFIX, LOGGER, Table, UUID_LENGTH, getClickhouseTableInfo}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.slf4j.LoggerFactory
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}

import java.util
import java.util.concurrent.ThreadLocalRandom
import java.util.{Objects, Properties, UUID}
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.{JavaConversions, mutable}
import scala.sys.process._


/**
 * Clickhouse sink use clickhouse-local program. Details see feature
 * <a href="https://github.com/apache/incubator-seatunnel/issues/1382">ST-1382</a> }
 */
class ClickhouseFile extends SparkBatchSink {

  private val properties: Properties = new Properties()
  private var clickhouseLocalPath: String = _
  private var table: Table = _
  private var fields: List[String] = _
  private var nodePass: Map[String, String] = _
  private val random = ThreadLocalRandom.current()
  private var freePass: Boolean = false

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {

    if (!config.hasPath("fields")) {
      this.fields = data.schema.fieldNames.toList
    }

    val session = env.getSparkSession
    import session.implicits._
    val encoder = Encoders.tuple(
      ExpressionEncoder[Shard],
      RowEncoder(data.schema))
    data.map(item => {
      val hashInstance = XXHashFactory.fastestInstance().hash64()
      val shard = getRowShard(distributedEngine.equals(this.table.engine), this.table.shards,
        this.table.shardKey, this.table.shardKeyType, this.table.shardWeightCount, this.random,
        hashInstance, item)
      (shard, item)
    })(encoder).groupByKey(si => si._1).mapGroups((shard, rows) => {
      // call clickhouse local
      val paths = generateClickhouseFile(rows)
      // move file
      moveFileToServer(shard, paths)
      // call attach
      attachClickhouseFile(shard, paths)
      // return result
      ""
    }).foreach(s => println(s))

  }

  private def generateClickhouseFile(rows: Iterator[(Shard, Row)]): List[String] = {
    val data = rows.map(r => {
      this.fields.map(f => r._2.getAs[Object](f).toString).mkString("\t")
    }).mkString("\n")

    def getValue(kv: util.Map.Entry[String, String]) = {
      if (this.fields.contains(kv.getKey)) {
        kv.getKey
      } else {
        val v = getDefaultValue(kv.getValue)
        if (v == null) {
          "null"
        } else if (v.isInstanceOf[Integer]) {
          v.toString
        } else {
          s"'${v.toString}'"
        }
      }
    }

    val uuid = UUID.randomUUID().toString.substring(0, UUID_LENGTH)


    val command = Process(Seq(s"echo", "-e", s"'$data'", "|", clickhouseLocalPath, "-S",
      s"'${fields.map(f => s"$f ${this.table.tableSchema.get(f)}").mkString(",")}'", "-N",
      s"'temp_table$uuid'", "-q", "\"", s"${this.table.localCreateTableDDL}; ",
      s"INSERT INTO TABLE ${this.table.name} ", "SELECT", s"${
        this.table.tableSchema.entrySet.map(getValue)
          .mkString(",")
      }", "FROM", s"temp_table$uuid;", "\"", "--path", s"$CLICKHOUSE_FILE_PREFIX/$uuid"))
    //    val command = s"echo -e '$data''" #| s"$clickhouseLocalPath -S " +
    //      s"'${fields.map(f => s"$f ${this.table.tableSchema.get(f)}")}' " +
    //      s"-N 'temp_table${uuid}' -q \"${this.table.localCreateTableDDL}; " +
    //      s"INSERT INTO TABLE ${this.table.name} " +
    //      s"SELECT ${this.table.tableSchema.entrySet.map(getValue).mkString(",")} FROM temp_table$uuid; \"" +
    //      s"--path $CLICKHOUSE_FILE_PREFIX/$uuid"
    LOGGER.info(command.lineStream.mkString("\n"))

    s"ls -d $CLICKHOUSE_FILE_PREFIX/$uuid/".lineStream.filter(s => !s.equals("detached")).toList
  }

  private def moveFileToServer(shard: Shard, paths: List[String]): Unit = {
    paths.foreach(path => {
      val jsch = new JSch()
      val session = jsch.getSession("root", shard.hostAddress)
      if (!this.freePass) {
        session.setPassword(nodePass(shard.hostAddress))
      }
      // TODO override Scp to support zero copy
      val scp = new Scp(session)
      scp.put(path, null, s"${this.table.dataPaths}/detached/", null)
    })
  }

  private def attachClickhouseFile(shard: Shard, paths: List[String]): Unit = {
    val balanced: BalancedClickhouseDataSource =
      new BalancedClickhouseDataSource(
        s"jdbc:clickhouse://${shard.hostAddress}:${shard.port}/${shard.database}", properties)
    val conn = balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
    paths.map(path => path.substring(CLICKHOUSE_FILE_PREFIX.length + UUID_LENGTH + 2)).foreach(part => {
      conn.createStatement().execute(s"ALTER TABLE ${this.table.shardTable} ATTACH PART '$part'")
    })
  }

  override def checkConfig(): CheckResult = {
    var checkResult = checkAllExists(config, "host", "table", "database", "username", "password",
      "clickhouse_local_path")
    if (checkResult.isSuccess) {
      clickhouseLocalPath = config.getString("clickhouse_local_path")
      properties.put("user", config.getString("username"))
      properties.put("password", config.getString("password"))
      val host = config.getString("host")
      val database = config.getString("database")
      val table = config.getString("table")
      val conn = getClickhouseConnection(host, database, properties)

      // 1. 获取create table 的信息
      val (result, tableInfo) = getClickhouseTableInfo(conn, database, table)
      if (!Objects.isNull(result)) {
        checkResult = result
      } else {
        this.table = tableInfo
        // 2. 获取table对应的node信息
        tableInfo.initTableInfo(host, conn)
        // 检查是否包含这些node的访问权限
        if (config.hasPath("node_free_password") && config.getBoolean("node_free_password")) {
          this.freePass = true
        } else if (config.hasPath("node_pass")) {
          val nodePass = config.getObjectList("node_pass")
          val nodePassMap = mutable.Map[String, String]()
          nodePass.foreach(np => {
            val address = np.toConfig.getString("node_address")
            val password = np.toConfig.getString("password")
            nodePassMap(address) = password
          })
          this.nodePass = nodePassMap.toMap
          checkResult = checkNodePass(this.nodePass, tableInfo.shards.values().toList)
        } else {
          checkResult = CheckResult.error("if clickhouse node is free password to spark node, " +
            "make config 'node_free_password' set true. Otherwise need provide clickhouse node password for" +
            " root user, location at node_pass config.")
        }
        if (checkResult.isSuccess) {
          // 3. 检查分片方式 相同分片的数据一定要生成在一起
          if (config.hasPath("sharding_key") && StringUtils.isNotEmpty(config.getString("sharding_key"))) {
            this.table.shardKey = config.getString("sharding_key")
          }
          checkResult = this.table.prepareShardInfo(conn)
          if (checkResult.isSuccess) {
            if (this.config.hasPath("fields")) {
              this.fields = config.getStringList("fields").toList
              checkResult = acceptedClickHouseSchema(this.fields, JavaConversions.mapAsScalaMap(this.table
                .tableSchema).toMap, this.table.name)
            }
          }
        }
      }
    }
    checkResult
  }

  private def checkNodePass(nodePassMap: Map[String, String], shardInfo: List[Shard]): CheckResult = {
    val noPassShard = shardInfo.filter(shard => !nodePassMap.contains(shard.hostAddress) &&
      !nodePassMap.contains(shard.hostname))
    if (noPassShard.nonEmpty) {
      CheckResult.error(s"can't find node ${
        String.join(",", JavaConversions.asJavaIterable(noPassShard.map(s => s.hostAddress)))
      } password in node_address config")
    } else {
      CheckResult.success()
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
  }
}


object ClickhouseFile {

  private final val CLICKHOUSE_FILE_PREFIX = "/tmp/clickhouse-local/spark-file"
  private val LOGGER = LoggerFactory.getLogger(classOf[ClickhouseFile])
  private val UUID_LENGTH = 10

  class Table(val name: String, val database: String, val engine: String, val createTableDDL: String, val
  engineFull: String, val dataPaths: String) extends Serializable {

    var shards = new util.TreeMap[Int, Shard]()
    var shardTable: String = name
    var shardWeightCount: Int = 0
    var shardKey: String = _
    var tableSchema: util.LinkedHashMap[String, String] = new util.LinkedHashMap[String, String]()
    var shardKeyType: String = _
    var localCreateTableDDL: String = createTableDDL

    def initTableInfo(host: String, conn: ClickHouseConnectionImpl): Unit = {
      if (shards.size() == 0) {
        val hostAndPort = host.split(":")
        if (distributedEngine.equals(this.engine)) {
          val localTable = getClickHouseDistributedTable(conn, database, name)
          this.shardTable = localTable.table
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

  private def getClickhouseTableInfo(conn: ClickHouseConnectionImpl, database: String, table: String):
  (CheckResult, Table) = {
    val sql = s"select engine,create_table_query,engine_full,data_paths from system.tables where database " +
      s"= '$database' and name = '$table'"
    val rs = conn.createStatement().executeQuery(sql)
    if (rs.next()) {
      (null, new Table(table, database, rs.getString(1), rs.getString(2),
        rs.getString(3), rs.getString(4)))
    } else {
      (CheckResult.error(s"can't find table '$table' in database '$database', please check config file"),
        null)
    }
  }

}
