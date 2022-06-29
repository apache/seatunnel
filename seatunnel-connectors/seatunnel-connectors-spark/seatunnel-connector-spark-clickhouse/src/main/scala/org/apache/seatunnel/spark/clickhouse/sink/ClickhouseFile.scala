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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.jpountz.xxhash.XXHashFactory
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.clickhouse.Config.{CLICKHOUSE_LOCAL_PATH, COPY_METHOD, DATABASE, FIELDS, HOST, NODE_ADDRESS, NODE_FREE_PASSWORD, NODE_PASS, PASSWORD, SHARDING_KEY, TABLE, TMP_BATCH_CACHE_LINE, USERNAME}
import org.apache.seatunnel.spark.clickhouse.sink.Clickhouse._
import org.apache.seatunnel.spark.clickhouse.sink.ClickhouseFile.{CLICKHOUSE_FILE_PREFIX, LOGGER, UUID_LENGTH, getClickhouseTableInfo}
import org.apache.seatunnel.spark.clickhouse.sink.filetransfer.{FileTransfer, RsyncFileTransfer, ScpFileTransfer}
import org.apache.seatunnel.spark.clickhouse.sink.filetransfer.TransferMethod.{RSYNC, SCP, TransferMethod, getCopyMethod}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.slf4j.LoggerFactory
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}
import java.util
import java.util.concurrent.ThreadLocalRandom
import java.util.{Objects, Properties, UUID}
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.{JavaConversions, mutable}
import scala.sys.process._
import scala.util.{Failure, Success, Try}


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
  private var copyFileMethod: TransferMethod = SCP
  private var tmpBatchCacheLine = 100000

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {

    if (!config.hasPath(FIELDS)) {
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
      val paths = generateClickhouseFile(rows)
      moveFileToServer(shard, paths)
      attachClickhouseFile(shard, paths)
      clearLocalFile(paths.head.substring(0, CLICKHOUSE_FILE_PREFIX.length + UUID_LENGTH + 1))
      0
    }).foreach(_ => {})

  }

  private def generateClickhouseFile(rows: Iterator[(Shard, Row)]): List[String] = {

    def getValue(kv: util.Map.Entry[String, String]): String = {
      if (this.fields.contains(kv.getKey)) {
        kv.getKey
      } else {
        val v = getDefaultValue(kv.getValue)
        if (v == null) {
          "NULL"
        } else if (v.isInstanceOf[Integer]) {
          "0"
        } else {
          s"'${v.toString}'"
        }
      }
    }

    val uuid = UUID.randomUUID().toString.substring(0, UUID_LENGTH).replaceAll("-", "_")
    val targetPath = java.lang.String.format("%s/%s", CLICKHOUSE_FILE_PREFIX, uuid)
    val target = new File(targetPath)
    target.mkdirs()
    val tmpDataPath = targetPath + "/local_data.log"

    mmapSaveDataSafely(tmpDataPath, rows.map(r => r._2))

    val exec = mutable.ListBuffer[String]()
    exec.appendAll(clickhouseLocalPath.trim.split(" "))
    exec.append("-S")
    exec.append(fields.map(f => s"$f ${this.table.tableSchema.get(f)}").mkString(","))
    exec.append("-N")
    exec.append("temp_table" + uuid)
    exec.append("-q")
    exec.append(java.lang.String.format("%s; INSERT INTO TABLE %s SELECT %s FROM temp_table%s;", this.table.getCreateDDLNoDatabase
      .replaceAll("`", ""), this.table.getLocalTableName,
      this.table.tableSchema.entrySet.map(getValue).mkString(","), uuid))
    exec.append("--path")
    exec.append(targetPath)
    val command = Process(Seq("less", tmpDataPath)) #| exec
    LOGGER.info(command.lineStream.mkString("\n"))

    new File(targetPath + "/data/_local/" + this.table.getLocalTableName).listFiles().filter(f => f.isDirectory).
      filterNot(f => f.getName.equals("detached")).map(f => f.getAbsolutePath).toList
  }

  private def mmapSaveDataSafely(path: String, rows: Iterator[Row]): Unit = {

    val outputChannel = FileChannel.open(Paths.get(path), StandardOpenOption.WRITE, StandardOpenOption.READ,
      StandardOpenOption.CREATE_NEW)
    val cache = mutable.ListBuffer[Row]()
    while (rows.hasNext) {
      cache.append(rows.next())
      if (cache.length >= tmpBatchCacheLine) {
        mmapSaveData(outputChannel, cache.toList)
        cache.clear()
      }
    }
    if (cache.nonEmpty) {
      mmapSaveData(outputChannel, cache.toList)
    }
    outputChannel.close()
  }

  private def mmapSaveData(outputChannel: FileChannel, rows: List[Row]): Unit = {
    val data = rows.map(r => {
      this.fields.map(f => r.getAs[Object](f).toString).mkString("\t") + "\n"
    }).mkString
    val buffer = outputChannel.map(FileChannel.MapMode.READ_WRITE, outputChannel.size(), data.getBytes.length)
    buffer.put(data.getBytes)
  }

  private def moveFileToServer(shard: Shard, paths: List[String]): Unit = {

    var fileTransfer: FileTransfer = null
    this.copyFileMethod match {
      case SCP => {
        if (freePass || !nodePass.contains(shard.hostAddress)) {
          fileTransfer = new ScpFileTransfer(shard.hostAddress)
        } else {
          fileTransfer = new ScpFileTransfer(shard.hostAddress, nodePass(shard.hostAddress))
        }
      }
      case RSYNC => {
        if (freePass || !nodePass.contains(shard.hostAddress)) {
          fileTransfer = new RsyncFileTransfer(shard.hostAddress)
        } else {
          fileTransfer = new RsyncFileTransfer(shard.hostAddress, nodePass(shard.hostAddress))
        }
      }
      case _ => throw new UnsupportedOperationException(s"unknown copy file method: '$copyFileMethod', please use " +
        s"scp/rsync instead")
    }
    fileTransfer.init()
    fileTransfer.transferAndChown(paths, s"${this.table.getLocalDataPath(shard).head}detached/")

    fileTransfer.close()
  }

  private def attachClickhouseFile(shard: Shard, paths: List[String]): Unit = {
    val balanced: BalancedClickhouseDataSource =
      new BalancedClickhouseDataSource(
        s"jdbc:clickhouse://${shard.hostAddress}:${shard.port}/${shard.database}", properties)
    val conn = balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]
    paths.map(path => fromPathGetPart(path)).foreach(part => {
      conn.createStatement().execute(s"ALTER TABLE ${this.table.getLocalTableName} ATTACH PART '$part'")
    })
  }

  private def fromPathGetPart(path: String): String = {
    path.substring(path.lastIndexOf("/") + 1)
  }

  private def clearLocalFile(path: String): Unit = {
    val r = Try(FileUtils.deleteDirectory(new File(path)))
    r match {
      case Failure(exception) =>
        LOGGER.warn(s"delete folder failed, path : $path", exception)
      case Success(_) =>
    }
  }

  override def checkConfig(): CheckResult = {
    var checkResult = checkAllExists(config, HOST, TABLE, DATABASE, USERNAME, PASSWORD,
      CLICKHOUSE_LOCAL_PATH)
    if (checkResult.isSuccess) {
      clickhouseLocalPath = config.getString(CLICKHOUSE_LOCAL_PATH)
      properties.put("user", config.getString(USERNAME))
      properties.put("password", config.getString(PASSWORD))
      val hosts = parseHost(config.getString(HOST))
      val database = config.getString(DATABASE)
      val table = config.getString(TABLE)
      val conn = getClickhouseConnection(hosts.map(_.hostAndPort).mkString(","), database, properties)

      if (config.hasPath(COPY_METHOD)) {
        this.copyFileMethod = getCopyMethod(config.getString(COPY_METHOD))
      }

      if (config.hasPath(TMP_BATCH_CACHE_LINE)) {
        this.tmpBatchCacheLine = config.getInt(TMP_BATCH_CACHE_LINE)
      }

      val (result, tableInfo) = getClickhouseTableInfo(conn, database, table)
      if (!Objects.isNull(result)) {
        checkResult = result
      } else {
        this.table = tableInfo
        tableInfo.initTableInfo(hosts, conn)
        tableInfo.initShardDataPath(config.getString(USERNAME), config.getString(PASSWORD))
        // check config of node password whether completed or not
        if (config.hasPath(NODE_FREE_PASSWORD) && config.getBoolean(NODE_FREE_PASSWORD)) {
          this.freePass = true
        } else if (config.hasPath(NODE_PASS)) {
          val nodePass = config.getObjectList(NODE_PASS)
          val nodePassMap = mutable.Map[String, String]()
          nodePass.foreach(np => {
            val address = np.toConfig.getString(NODE_ADDRESS)
            val password = np.toConfig.getString(PASSWORD)
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
          // check sharding method
          if (config.hasPath(SHARDING_KEY) && StringUtils.isNotEmpty(config.getString(SHARDING_KEY))) {
            this.table.shardKey = config.getString(SHARDING_KEY)
          }
          checkResult = this.table.prepareShardInfo(conn)
          if (checkResult.isSuccess) {
            if (this.config.hasPath(FIELDS)) {
              this.fields = config.getStringList(FIELDS).toList
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

  override def getPluginName: String = "ClickhouseFile"
}


object ClickhouseFile {

  private final val CLICKHOUSE_FILE_PREFIX = "/tmp/clickhouse-local/spark-file"
  private val LOGGER = LoggerFactory.getLogger(classOf[ClickhouseFile])
  private val UUID_LENGTH = 10
  private val OBJECT_MAPPER = new ObjectMapper()
  OBJECT_MAPPER.registerModule(DefaultScalaModule)

  def getClickhouseTableInfo(conn: ClickHouseConnectionImpl, database: String, table: String):
  (CheckResult, Table) = {
    val sql = s"select engine,create_table_query,engine_full,data_paths from system.tables where database " +
      s"= '$database' and name = '$table'"
    val rs = conn.createStatement().executeQuery(sql)
    if (rs.next()) {
      (null, new Table(table, database, rs.getString(1), rs.getString(2),
        rs.getString(3),
        OBJECT_MAPPER.readValue(rs.getString(4).replaceAll("'", "\""),
          classOf[util.List[String]]).toList))
    } else {
      (CheckResult.error(s"can't find table '$table' in database '$database', please check config file"),
        null)
    }
  }

}
