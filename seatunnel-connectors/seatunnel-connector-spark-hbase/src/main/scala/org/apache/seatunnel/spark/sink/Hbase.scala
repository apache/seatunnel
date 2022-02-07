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

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

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
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes

class Hbase extends SparkBatchSink with Logging {

  @transient var hbaseConf: Configuration = _
  var hbaseContext: HBaseContext = _
  var hbasePrefix = "hbase."

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "hbase.zookeeper.quorum", "catalog", "staging_dir")
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> HbaseSaveMode.Append.toString.toLowerCase))

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
      dfWithStringFields =
        dfWithStringFields.withColumn(colName, col(colName).cast(DataTypes.StringType))
    }

    val parameters = Map(HBaseTableCatalog.tableCatalog -> catalog)
    val htc = HBaseTableCatalog(parameters)
    val tableName = TableName.valueOf(htc.namespace + ":" + htc.name)
    val columnFamily = htc.getColumnFamilies
    val saveMode = config.getString("save_mode").toLowerCase
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

      hbaseContext.bulkLoadThinRows[Row](
        dfWithStringFields.rdd,
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
      load.doBulkLoad(
        new Path(stagingDir),
        hbaseConn.getAdmin,
        table,
        hbaseConn.getRegionLocator(tableName))

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
      logWarning(s"clean staging dir ${stagingDir} failed")
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
}
