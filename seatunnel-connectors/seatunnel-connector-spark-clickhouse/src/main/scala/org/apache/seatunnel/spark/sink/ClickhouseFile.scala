package org.apache.seatunnel.spark.sink

import net.jpountz.xxhash.XXHashFactory
import org.apache.commons.lang3.StringUtils
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.sink.Clickhouse._
import org.apache.seatunnel.spark.sink.ClickhouseFile.Table
import org.apache.spark.sql.{Dataset, Row}
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnectionImpl}

import java.util
import java.util.concurrent.ThreadLocalRandom
import java.util.{Objects, Properties}
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.{JavaConversions, mutable}


/**
 * Clickhouse sink use clickhouse-local program. Details see feature
 * <a href="https://github.com/apache/incubator-seatunnel/issues/1382">ST-1382</a> }
 */
class ClickhouseFile extends SparkBatchSink {

  private val properties: Properties = new Properties()
  private var clickhouseLocalPath: String = _
  private var table: Table = _
  private var fields: util.List[String] = _
  private val random = ThreadLocalRandom.current()

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val session = env.getSparkSession
    import session.implicits._
    data.map(item => {
      val hashInstance = XXHashFactory.fastestInstance().hash64()
      val shard = getRowShard(distributedEngine.equals(this.table.engine), this.table.shards,
        this.table.shardKey, this.table.shardKeyType, this.table.shardWeightCount, this.random,
        hashInstance, item)
      (shard, item)
    }).groupByKey(si => si._1.hostAddress)

  }

  override def checkConfig(): CheckResult = {
    var checkResult = checkAllExists(config, "host", "table", "database", "username", "password",
      "clickhouse_local_path")
    if (checkResult.isSuccess) {
      clickhouseLocalPath = config.getString("clickhouse_local_path")
      properties.put("user", config.getString("username"))
      properties.put("password", config.getString("password"))
      val host = config.getString("host")
      val globalJdbc = String.format("jdbc:clickhouse://%s/", host)
      val balanced: BalancedClickhouseDataSource = new BalancedClickhouseDataSource(globalJdbc, properties)
      val conn = balanced.getConnection.asInstanceOf[ClickHouseConnectionImpl]

      // 1. 获取create table 的信息
      val database = config.getString("database")
      val table = config.getString("table")
      val (result, tableInfo) = getClickhouseTableInfo(conn, database, table)
      if (!Objects.isNull(result)) {
        checkResult = result
      } else {
        this.table = tableInfo
        // 2. 获取table对应的node信息
        val shardInfo = tableInfo.getTableShard(host, conn)
        // 检查是否包含这些node的访问权限
        val nodePass = config.getObjectList("node_pass")
        val nodePassMap = mutable.Map[String, String]()
        nodePass.foreach(np => {
          val address = np.get("node_address").toString
          val password = np.get("password").toString
          nodePassMap(address) = password
        })
        checkResult = checkNodePass(nodePassMap.toMap, shardInfo)
        if (checkResult.isSuccess) {
          // 3. 检查分片方式 相同分片的数据一定要生成在一起
          if (config.hasPath("sharding_key") && StringUtils.isNotEmpty(config.getString("sharding_key"))) {
            this.table.shardKey = config.getString("sharding_key")
          }
          checkResult = this.table.prepareShardInfo(conn)
          if (checkResult.isSuccess) {
            if (this.config.hasPath("fields")) {
              this.fields = config.getStringList("fields")
              checkResult = acceptedClickHouseSchema(this.fields.toList, this.table.tableSchema, this.table
                .name)
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

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
  }
}


object ClickhouseFile {
  class Table(val name: String, val database: String, val engine: String, val createTableQuery: String, val
  engineFull: String, val dataPaths: String) extends Serializable {

    var shards = new util.TreeMap[Int, Shard]()
    var shardTable: String = name
    var shardWeightCount: Int = 0
    var shardKey: String = _
    var tableSchema: Map[String, String] = Map[String, String]()
    var shardKeyType: String = _

    def getTableShard(host: String, conn: ClickHouseConnectionImpl): List[Shard] = {
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
        } else {
          this.shards.put(0, new Shard(1, 1, 1, hostAndPort(0),
            hostAndPort(0), hostAndPort(1), database))
        }
      }
      this.shards.values().toList
    }

    def prepareShardInfo(conn: ClickHouseConnectionImpl): CheckResult = {
      this.tableSchema = getClickHouseSchema(conn, name)
      if (StringUtils.isNotEmpty(this.shardKey)) {
        if (!this.tableSchema.contains(this.shardKey)) {
          CheckResult.error(
            s"not find field '${this.shardKey}' in table '${this.name}' as sharding key")
        } else {
          this.shardKeyType = this.tableSchema(this.shardKey)
          CheckResult.success()
        }
      } else {
        CheckResult.success()
      }
    }
  }
}
