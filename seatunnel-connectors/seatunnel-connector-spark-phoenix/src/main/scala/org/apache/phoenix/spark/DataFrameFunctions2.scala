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
package org.apache.phoenix.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.phoenix.spark.CsUtil._
import org.apache.phoenix.util.SchemaUtil
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConversions._

class DataFrameFunctions2(data: Dataset[Row]) extends Serializable {

  def saveToPhoenix(
      tableName: String,
      conf: Configuration = new Configuration,
      zkUrl: Option[String] = None,
      tenantId: Option[String] = None,
      skipNormalizingIdentifier: Boolean = false): Unit = {

    val config = HBaseConfiguration.create
    val job = Job.getInstance(config)
    TableMapReduceUtil.initCredentials(job)

    val spark = data.sparkSession
    val c1 = Some(spark.sparkContext.broadcast(new SerializableWritable(job.getCredentials)))

    val fieldArray = getFieldArray(skipNormalizingIdentifier, data)

    @transient val outConfig =
      ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrl, tenantId, Some(conf))

    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    val phxRDD = data.rdd.mapPartitions { rows =>
      applyCs(c1)
      @transient val partitionConfig =
        ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrlFinal, tenantId)
      @transient val columns =
        PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig).toList

      rows.map { row =>
        val rec = new PhoenixRecordWritable(columns)
        row.toSeq.foreach { e => rec.add(e) }
        (null, rec)
      }
    }

    phxRDD.saveAsNewAPIHadoopFile(
      "",
      classOf[NullWritable],
      classOf[PhoenixRecordWritable],
      classOf[PhoenixOutputFormat[PhoenixRecordWritable]],
      outConfig)
  }

  def getFieldArray(skipNormalizingIdentifier: Boolean = false, data: DataFrame): Array[String] = {
    if (skipNormalizingIdentifier) {
      data.schema.fieldNames.map(x => x)
    } else {
      data.schema.fieldNames.map(x => SchemaUtil.normalizeIdentifier(x))
    }
  }

}
