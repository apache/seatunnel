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

import java.sql.Timestamp

import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.seatunnel.spark.transform.NulltfConfig._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap}

class Nulltf extends BaseSparkTransform {


  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {

    var fieldNameDefault = new mutable.HashMap[String, String]()
    if (!config.getConfig(FIELDS).isEmpty) {
      config.getConfig(FIELDS).entrySet().foreach(kv => {
        fieldNameDefault += (kv.getKey -> kv.getValue.unwrapped().toString)
      })
    }

    df.mapPartitions(iter => {
      var result = ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row = iter.next()
        val fieldSeq = mutable.Buffer[Any]()
        for (i <- 0 until row.size) {
          val newField = if (row.isNullAt(i)) {
            val fieldName = row.schema.fields.apply(i).name
            val fieldType = row.schema.fields.apply(i).dataType

            val temp = fieldNameDefault.get(fieldName)
            if (temp.isDefined) {
              if (temp.get == null) getDefaultValueByDataType(fieldType) else transformStringToRightType(temp.get, fieldType)
            } else {
              getDefaultValueByDataType(fieldType)
            }
          } else row.get(i)

          fieldSeq += newField
        }
        val newRow = Row.fromSeq(fieldSeq)
        result += newRow
      }
      result.iterator
    })(RowEncoder.apply(df.schema))

  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config)
  }

  override def getPluginName: String = PLUGIN_NAME

  private def getDefaultValueByDataType(dataType: DataType): Any = {
    dataType match {
      case StringType => ""
      case ShortType => 0
      case IntegerType => 0
      case FloatType => 0f
      case DoubleType => 0d
      case LongType => 0L
      case BooleanType => false
      case DateType => new java.sql.Date(System.currentTimeMillis())
      case TimestampType => new Timestamp(System.currentTimeMillis())
      case _ => null
    }
  }

  private def transformStringToRightType(value: String, dataType: DataType): Any = {
    dataType match {
      case StringType => value
      case ShortType => java.lang.Short.valueOf(value)
      case IntegerType => java.lang.Integer.valueOf(value)
      case FloatType => java.lang.Float.valueOf(value)
      case DoubleType => java.lang.Double.valueOf(value)
      case LongType => java.lang.Long.valueOf(value)
      case BooleanType => java.lang.Boolean.valueOf(value)
      case DateType => java.sql.Date.valueOf(value)
      case TimestampType => java.sql.Timestamp.valueOf(value)
      case _ => value
    }
  }


}
