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
package org.apache.seatunnel.spark.hbase.sink

import java.nio.charset.StandardCharsets
import scala.collection.JavaConversions.{asScalaSet,mapAsJavaMap}
import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.spark.{ByteArrayWrapper, FamiliesQualifiersValues, HBaseContext}
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.hbase.Config.{CATALOG, HBASE_ZOOKEEPER_QUORUM, NULLABLE, SAVE_MODE, STAGING_DIR}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, MapType}

class Hbase extends SparkBatchSink with Logging {

  @transient var hbaseConf: Configuration = _
  var hbaseContext: HBaseContext = _
  var hbasePrefix = "hbase."
  var zookeeperPrefix = "zookeeper."
  var MAP_TYPE = "map"
  override def checkConfig(): CheckResult = {
    checkAllExists(config, HBASE_ZOOKEEPER_QUORUM, CATALOG, STAGING_DIR)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        SAVE_MODE -> HbaseSaveMode.Append.toString.toLowerCase,
        NULLABLE -> false))

    config = config.withFallback(defaultConfig)
    hbaseConf = HBaseConfiguration.create(env.getSparkSession.sessionState.newHadoopConf())
    config
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        if (key.startsWith(hbasePrefix) || key.startsWith(zookeeperPrefix)) {
          val value = String.valueOf(entry.getValue.unwrapped())
          hbaseConf.set(key, value)
        }
      })
    hbaseContext = new HBaseContext(env.getSparkSession.sparkContext, hbaseConf)
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    var dfWithFields = df
    val colNames = df.columns
    val catalog = config.getString(CATALOG)
    val stagingDir = config.getString(STAGING_DIR) + "/" + System.currentTimeMillis().toString
    val nullable = config.getBoolean(NULLABLE)
    var mapKeyType = DataTypes.StringType
    var mapValueType = DataTypes.IntegerType

    for (colName <- colNames) {
      if (df.schema(colName).dataType.typeName == MAP_TYPE) {
        mapKeyType = df.schema(colName).dataType.asInstanceOf[MapType].keyType
        mapValueType = df.schema(colName).dataType.asInstanceOf[MapType].valueType
      } else {
        dfWithFields =
          dfWithFields.withColumn(colName, col(colName).cast(DataTypes.StringType))
      }
    }

    val parameters = Map(HBaseTableCatalog.tableCatalog -> catalog)
    val htc = HBaseTableCatalog(parameters)
    val tableName = TableName.valueOf(htc.namespace + ":" + htc.name)
    val columnFamily = htc.getColumnFamilies
    val saveMode = config.getString(SAVE_MODE).toLowerCase
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)

    try {
      if (saveMode == HbaseSaveMode.Overwrite.toString.toLowerCase) {
        truncateHTable(hbaseConn, tableName)
      }

      def familyQualifierToByte: Set[(Array[Byte], Array[Byte], String)] = {
        if (columnFamily == null || colNames == null) {
          throw new Exception("null can't be convert to Bytes")
        }
        colNames.filter(htc.getField(_).cf != HBaseTableCatalog.rowKey).map(colName =>
          (Bytes.toBytes(htc.getField(colName).cf), Bytes.toBytes(colName), colName)).toSet
      }

      def mapTypeParser(row: Row, colName: String): Map[Any, Any] = {
        (mapKeyType, mapValueType) match {
          case (DataTypes.IntegerType, DataTypes.IntegerType) =>
            row.getAs[Map[Int, Int]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.IntegerType, DataTypes.LongType) =>
            row.getAs[Map[Int, Long]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.IntegerType, DataTypes.FloatType) =>
            row.getAs[Map[Int, Float]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.IntegerType, DataTypes.DoubleType) =>
            row.getAs[Map[Int, Double]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.IntegerType, DataTypes.StringType) =>
            row.getAs[Map[Int, String]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.StringType, DataTypes.IntegerType) =>
            row.getAs[Map[String, Int]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.StringType, DataTypes.LongType) =>
            row.getAs[Map[String, Long]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.StringType, DataTypes.FloatType) =>
            row.getAs[Map[String, Float]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.StringType, DataTypes.DoubleType) =>
            row.getAs[Map[String, Double]](colName).map { case (k, v) => (k, v) }
          case (DataTypes.StringType, DataTypes.StringType) =>
            row.getAs[Map[String, String]](colName).map { case (k, v) => (k, v) }
          case _ =>
            throw new Exception("Unsupported map type: " + mapKeyType.typeName + "," + mapValueType.typeName)
        }
      }

      hbaseContext.bulkLoadThinRows[Row](
        dfWithFields.rdd,
        tableName,
        r => {
          val rawPK = new StringBuilder
          for (c <- htc.getRowKey) {
            rawPK.append(r.getAs[String](c.colName))
          }

          val rkBytes = rawPK.toString.getBytes()
          val familyQualifiersValues = new FamiliesQualifiersValues
          val fq = familyQualifierToByte

          for (c <- r.schema.fields) {
            val colName = c.name
            val colType = c.dataType.typeName

            if (colType == MAP_TYPE) {
              val map = mapTypeParser(r, colName)
              for (entry <- map.entrySet()) {
                val key = entry.getKey.toString
                val value = entry.getValue.toString
                val familyQualifier = fq.filter(_._3 == colName).head

                familyQualifiersValues.add(
                  familyQualifier._1,
                  Bytes.toBytes(key),
                  Bytes.toBytes(value))
              }
            } else {
              breakable {
                var familyQualifier = new Tuple3[Array[Byte], Array[Byte], String](null, null, null)
                try {
                  familyQualifier = fq.filter(_._3 == colName).head
                } catch {
                  case e: Exception =>
                    log.warn("WARN: Not found content in the familyQualifier, Skip:" + e.getMessage)
                    break
                }

                val colValue = r.getAs[String](colName)
                familyQualifiersValues.add(
                  familyQualifier._1,
                  familyQualifier._2,
                  Bytes.toBytes(colValue))
                val colBytes = colValue.getBytes(StandardCharsets.UTF_8)
                val (family, qualifier, _) = fq.find(_._3 == colName).get
                familyQualifiersValues.add(family, qualifier, colBytes)
              }
            }
          }
          (new ByteArrayWrapper(rkBytes), familyQualifiersValues)
        },
        stagingDir)

      val load = new LoadIncrementalHFiles(hbaseConf)
      val table = hbaseConn.getTable(tableName)
      load.doBulkLoad(
        new Path(stagingDir),
        hbaseConn.getAdmin,
        table,
        hbaseConn.getRegionLocator(tableName))
    } catch {
      case e: Exception => {
        log.error("hbase bulk load failed", e)
        throw e
      }
    } finally {
      if (hbaseConn != null) {
        hbaseConn.close()
      }

      cleanUpStagingDir(stagingDir)
    }
  }

  private def cleanUpStagingDir(stagingDir: String): Unit = {
    val stagingPath = new Path(stagingDir)
    val fs = stagingPath.getFileSystem(hbaseContext.config)
    if (!fs.delete(stagingPath, true)) {
      logWarning(s"clean staging dir $stagingDir failed")
    }
    if (fs != null) {
      fs.close()
    }
  }

  private def truncateHTable(connection: Connection, tableName: TableName): Unit = {
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.truncateTable(tableName, true)
    }
  }

  override def getPluginName: String = "Hbase"
}
