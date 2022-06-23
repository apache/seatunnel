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

package org.apache.seatunnel.spark.transform

import org.apache.seatunnel.apis.base.plugin.Plugin
import org.apache.seatunnel.common.config.CheckConfigUtil._
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class NullRate extends BaseSparkTransform {

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {

    val allCount = env.getSparkSession.sparkContext.longAccumulator("allCount")
    val fieldsAndRates = config.getStringList(NullRateConfig.FIELDS).zip(config.getDoubleList(NullRateConfig.RATES)).filter(fl => df.schema.names.contains(fl._1)).toMap
    val fieldsAndRatesAccumulator = fieldsAndRates.map(fl => {
      fl._1 -> env.getSparkSession.sparkContext.longAccumulator(fl._1)
    })

    df.foreachPartition(iter => {
      while (iter.hasNext) {
        allCount.add(1L)
        val row = iter.next()
        fieldsAndRates.map(fl => fl._1).foreach(field => {
          val accumulator = fieldsAndRatesAccumulator.get(field).get
          if (row.get(row.fieldIndex(field)) == null) {
            accumulator.add(1L)
          } else {
            accumulator.add(0L)
          }
        })
      }
    })

    val allCountValue = allCount.value * 1.00d
    val nullRateValue = fieldsAndRatesAccumulator.map(fl => {
      (fl._1, fieldsAndRates.getOrDefault(fl._1, 100.00d), fl._2.value, (fl._2.value / allCountValue) * 100d)
    })

    if (config.hasPath(NullRateConfig.IS_THROWEXCEPTION) && config.getBoolean(NullRateConfig.IS_THROWEXCEPTION)) {
      nullRateValue.foreach(fv => {
        if (fv._4 > fv._2) {
          throw new RuntimeException(s"the field(${fv._1}) null rate(${fv._4}) is lager then the setting(${fv._2})")
        }
      })
    }

    if (config.hasPath(NullRateConfig.SAVE_TO_TABLE_NAME)) {
      val nullRateRows = nullRateValue.map(fv => {
        Row(fv._1, fv._2, fv._3, fv._4)
      }).toSeq

      val schema = new StructType()
        .add("field_name", DataTypes.StringType)
        .add("setting_rate", DataTypes.DoubleType)
        .add("null_count", DataTypes.LongType)
        .add("rate_percent", DataTypes.DoubleType)
      env.getSparkSession.createDataset(nullRateRows)(RowEncoder(schema)).createOrReplaceTempView(config.getString(NullRateConfig.SAVE_TO_TABLE_NAME))
    }

    df
  }

  override def checkConfig(): CheckResult = {
    val exists = checkAllExists(config, NullRateConfig.FIELDS, NullRateConfig.RATES)
    val equal = if (config.getStringList(NullRateConfig.FIELDS).size() == config.getIntList(NullRateConfig.RATES).size()) CheckResult.success()
    else CheckResult.error(s"the ${NullRateConfig.FIELDS} length is not equal ${NullRateConfig.RATES} length")
    val unique = if (config.getStringList(NullRateConfig.FIELDS).toList.distinct.size() == config.getStringList(NullRateConfig.FIELDS).size()) CheckResult.success()
    else CheckResult.error(s"the ${NullRateConfig.FIELDS} is not unique")
    mergeCheckResults(exists, equal, unique)
  }


  override def getPluginName: String = "NullRate"
}
