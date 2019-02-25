package io.github.interestinglab.waterdrop.output

import java.util

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingOutput
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql._
import org.bson.Document

import scala.collection.JavaConversions._

class MongoDBSink extends BaseStructuredStreamingOutput {

  var config: Config = ConfigFactory.empty()

  var mongoCollection: MongoCollection[Document] = _
  var client: MongoClient = _

  var updateFields: util.List[String] = _
  var options = new collection.mutable.HashMap[String, String]
  val outConfPrefix = "outConfig"

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("host") && config.hasPath("database") && config.hasPath("collection") match {
      case true => (true, "")
      case false => (false, "please specify [host]  and [database] and [collection]")
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
        "port" -> 27017,
        "mongo_output_mode" -> "insert",
        "streaming_output_mode" -> "append"
      )
    )
    config = config.withFallback(defaultConfig)
    config.hasPath(outConfPrefix) match {
      case true => {
        config
          .getConfig(outConfPrefix)
          .entrySet()
          .foreach(entry => {
            val key = entry.getKey
            val value = String.valueOf(entry.getValue.unwrapped())
            options.put(key, value)
          })
      }
      case false => {}
    }
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    client = new MongoClient(config.getString("host"), config.getInt("port"))
    mongoCollection = client
      .getDatabase(config.getString("database"))
      .getCollection(config.getString("collection"))
    true
  }

  override def process(value: Row): Unit = {
    //这种方式没实现
    //    val sparkSession = SparkSession.builder().getOrCreate()
    //    val rows = List[Row](value)
    //    val rowEncoder = RowEncoder(value.schema)
    //    val dataSet = sparkSession.createDataset(rows)(rowEncoder)
    //    MongoSpark.save(dataSet,writeConfig)

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

  override def close(errorOrNull: Throwable): Unit = {
    client.close()
  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] = {
    df.writeStream
      .outputMode(config.getString("streaming_output_mode"))
      .foreach(this)
      .options(options)
  }
}
