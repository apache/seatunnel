/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.util

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.output.utils.{MongoClientUtil, StructuredUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.bson.Document

import scala.collection.JavaConversions._

class MongoDB extends BaseStructuredStreamingOutput {

  var config: Config = ConfigFactory.empty()

  var mongoCollection: MongoCollection[Document] = _
  var client: Broadcast[MongoClientUtil] = _
  var mongoConfig: Config = _
  var updateFields: util.List[String] = _
  var options = new collection.mutable.HashMap[String, String]
  val mongoPrefix = "writeconfig."
  val outConfPrefix = "output.option."

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    mongoConfig = TypesafeConfigUtils.extractSubConfig(config, mongoPrefix, false)
    mongoConfig.hasPath("host") && mongoConfig.hasPath("database") && mongoConfig.hasPath("collection") match {
      case true => {
        StructuredUtils.checkTriggerMode(config) match {
          case true => (true, "")
          case false => (false, "please specify [interval] when [trigger_type] is ProcessingTime or Continuous")
        }
      }
      case false => (false, "please specify [writeconfig.host]  and [writeconfig.database] and [writeconfig.collection]")
    }

  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    config.hasPath("update_fields") match {
      case true => {
        updateFields = config.getStringList("update_fields")
      }
      case false => {}
    }
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        mongoPrefix + "port" -> 27017,
        "mongo_output_mode" -> "insert",
        "streaming_output_mode" -> "append",
        "trigger_type" -> "default"
      )
    )
    config = config.withFallback(defaultConfig)
    TypesafeConfigUtils.hasSubConfig(config,outConfPrefix) match {
      case true => {
        TypesafeConfigUtils.extractSubConfig(config, outConfPrefix, false)
          .entrySet()
          .foreach(entry => {
            val key = entry.getKey
            val value = String.valueOf(entry.getValue.unwrapped())
            options.put(key, value)
          })
      }
      case false => {}
    }
    client = spark.sparkContext.broadcast(MongoClientUtil(mongoConfig))
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    mongoCollection = client.value.getClient
      .getDatabase(config.getString("writeconfig.database"))
      .getCollection(config.getString("writeconfig.collection"))
    true
  }

  override def process(value: Row): Unit = {

    val fieldNames = value.schema.fieldNames
    val document = new Document()
    val query = new Document()
    fieldNames.foreach(name => {
      val v = value.get(value.fieldIndex(name))
      document.put(name, v)
      updateFields != null && updateFields.contains(name) match {
        case true => query.put(name, v)
        case false => {}
      }
    })
    val update = new Document("$set", document)
    config.getString("mongo_output_mode") match {
      case "insert" => mongoCollection.insertOne(document)
      case "updateOne" => mongoCollection.updateOne(query, update)
      case "updateMany" => mongoCollection.updateMany(query, update)
      case "upsert" => mongoCollection.updateOne(query, update, new UpdateOptions().upsert(true))
      case "replace" => mongoCollection.replaceOne(query, update, new UpdateOptions().upsert(true))
      case _ => throw new Exception("unknown write_mongo_mode ")
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {

    var writer = df.writeStream
      .outputMode(config.getString("streaming_output_mode"))
      .foreach(this)
      .options(options)

    writer = StructuredUtils.setCheckpointLocation(writer, config)
    StructuredUtils.writeWithTrigger(config, writer)
  }
}
