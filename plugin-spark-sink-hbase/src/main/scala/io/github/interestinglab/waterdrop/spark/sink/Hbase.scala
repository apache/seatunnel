package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.hadoop.hbase.spark.{ByteArrayWrapper, FamiliesQualifiersValues, HBaseContext}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, _}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

class Hbase extends SparkBatchSink with Logging {

  @transient var hbaseConf: Configuration = _
  var hbaseContext: HBaseContext = _
  var hbasePrefix = "hbase."

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

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("hbase.zookeeper.quorum", "catalog", "staging_dir")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }
    if (nonExistsOptions.length != 0) {
      new CheckResult(false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    } else {
      new CheckResult(true, "")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> HbaseSaveMode.Append.toString.toLowerCase
      )
    )

    config = config.withFallback(defaultConfig)
    hbaseConf = HBaseConfiguration.create(env.getSparkSession.sessionState.newHadoopConf())
    config
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        if (key.startsWith(hbasePrefix)) {
          val value = String.valueOf(entry.getValue.unwrapped())
          hbaseConf.set(key, value)
        }
      })
    hbaseContext = new HBaseContext(env.getSparkSession.sparkContext, hbaseConf)
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    var dfWithStringFields = df
    val colNames = df.columns
    val catalog = config.getString("catalog")
    val stagingDir = config.getString("staging_dir") + "/" + System.currentTimeMillis().toString

    // convert all columns type to string
    for (colName <- colNames) {
      dfWithStringFields = dfWithStringFields.withColumn(colName, col(colName).cast(DataTypes.StringType))
    }

    val parameters = Map(HBaseTableCatalog.tableCatalog -> catalog)
    val htc = HBaseTableCatalog(parameters)
    val tableName = TableName.valueOf(htc.namespace + ":" + htc.name)
    val columnFamily = htc.getColumnFamilies
    val save_mode = config.getString("save_mode").toLowerCase
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)

    try {
      if (save_mode == HbaseSaveMode.Overwrite.toString.toLowerCase) {
        truncateHTable(hbaseConn, tableName)
      }

      def familyQualifierToByte: Set[(Array[Byte], Array[Byte], String)] = {
        if (columnFamily == null || colNames == null) throw new Exception("null can't be convert to Bytes")
        colNames.filter(htc.getField(_).cf != HBaseTableCatalog.rowKey).map(colName => (Bytes.toBytes(htc.getField(colName).cf), Bytes.toBytes(colName), colName)).toSet
      }

      hbaseContext.bulkLoadThinRows[Row](dfWithStringFields.rdd,
        tableName,
        r => {
          val rawPK = new StringBuilder
          for (c <- htc.getRowKey) {
            rawPK.append(r.getAs[String](c.colName))
          }

          val rkBytes = rawPK.toString.getBytes()
          val familyQualifiersValues = new FamiliesQualifiersValues
          val fq = familyQualifierToByte
          for (c <- fq) {
            breakable {
              val family = c._1
              val qualifier = c._2
              val value = r.getAs[String](c._3)
              if (value == null) {
                break
              }
              familyQualifiersValues += (family, qualifier, Bytes.toBytes(value))
            }
          }
          (new ByteArrayWrapper(rkBytes), familyQualifiersValues)
        },
        stagingDir)

      val load = new LoadIncrementalHFiles(hbaseConf)
      val table = hbaseConn.getTable(tableName)
      load.doBulkLoad(new Path(stagingDir), hbaseConn.getAdmin, table,
        hbaseConn.getRegionLocator(tableName))

    } finally {
      if (hbaseConn != null)
        hbaseConn.close()

      cleanUpStagingDir(stagingDir)
    }
  }

  private def cleanUpStagingDir(stagingDir: String): Unit = {
    val stagingPath = new Path(stagingDir)
    val fs = stagingPath.getFileSystem(hbaseContext.config)
    if (!fs.delete(stagingPath, true)) {
      logWarning(s"clean staging dir ${stagingDir} failed")
    }
    if (fs != null)
      fs.close()
  }

  private def truncateHTable(connection: Connection, tableName: TableName): Unit = {
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.truncateTable(tableName, true)
    }
  }
}
