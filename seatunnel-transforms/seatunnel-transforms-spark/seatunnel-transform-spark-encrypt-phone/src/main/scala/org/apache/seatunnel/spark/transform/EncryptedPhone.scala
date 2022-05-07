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

import java.util

import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._

class EncryptedPhone extends BaseSparkTransform {
  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val encryptedFields = if (config.hasPath("fields")) {
      val configFields = config.getStringList("fields")
      df.schema.map(_.name).toList.map(field => {
        (field, configFields.contains(field))
      })
    } else {
      df.schema.map(field => (field.name, true)).toList
    }

    df.mapPartitions(new MapPartitionsFunction[Row, Row] {
      override def call(input: util.Iterator[Row]): util.Iterator[Row] = {
        def encrypted(value: Any): Any = {
          value match {
            case stringValue: String => containsAndEncrypted(stringValue)
            case stringValue: UTF8String => UTF8String.fromString(containsAndEncrypted(stringValue.toString))
            case _ => value
          }
        }

        def containsAndEncrypted(number: String): String = {
          val regex = """.*(13|14|15|17|18|19)([0-9]{9}).*""".r
          number match {
            case regex(_, tail) => {
              number.replace(tail.substring(1, 5), "****")
            }
            case _ => number
          }
        }

        input.toList.map(row => {
          encryptedFields.map(encryptedField => {
            val index = row.fieldIndex(encryptedField._1)
            if (encryptedField._2) {
              encrypted(row.get(index))
            } else {
              row.get(index)
            }
          })
        }).map(x => Row(x: _*)).toIterator
      }
    }, RowEncoder.apply(df.schema))
  }

  override def getPluginName: String = "encryptedPhone"
}
