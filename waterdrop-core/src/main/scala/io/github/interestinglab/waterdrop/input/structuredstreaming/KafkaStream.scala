package io.github.interestinglab.waterdrop.input.structuredstreaming

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStructuredStreamingInput
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

class KafkaStream extends BaseStructuredStreamingInput {
  var config: Config = ConfigFactory.empty()
  var schema = new StructType()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  // kafka consumer configuration : http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  val consumerPrefix = "consumer"

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("consumer.bootstrap.servers") && config.hasPath("topics") match {
      case true => (true, "")
      case false => (false, "please specify [consumer.bootstrap.servers] and [topics] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit ={
    config.hasPath("schema") match {
      case true => {
        val schemaJson = JSON.parseObject(config.getString("schema"))
        schema = getSchema(schema,schemaJson)
      }
      case false =>{}
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val topics = config.getString("topics")
    val consumerConfig = config.getConfig(consumerPrefix)
    val kafkaParams = consumerConfig
      .entrySet()
      .foldRight(Map[String, String]())((entry, map) => {
        map + (entry.getKey -> entry.getValue.unwrapped().toString)
      })

    println("[INFO] Input Kafka Params:")
    for (entry <- kafkaParams) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
    val dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("consumer.bootstrap.servers"))
      .option("subscribe", topics)
      .options(kafkaParams)
      .load()
    if (schema.size > 0){
      dataFrame.withColumn("parse_value",from_json(col("value"),schema))
    }
    dataFrame
  }

  private def getSchema(schema: StructType,jsonSchema: JSON): StructType ={
    for(entry <- jsonSchema){
      val (field,dataType) = entry
      dataType.isInstanceOf[JSON] match {
        case true => {
          val st = getSchema(new StructType(),dataType.asInstanceOf[JSON])
          schema.add(field,st)
        }
        case false => schema.add(field,getType(dataType.asInstanceOf[String]))
      }
    }
    schema
  }

  private def  getType(typeString:String): DataType = {
    var dataType = DataTypes.NullType;
    typeString.toLowerCase() matches {
      case "string" => dataType = DataTypes.StringType
      case "integer" => dataType = DataTypes.IntegerType
      case "long" => dataType = DataTypes.LongType
      case "double" => dataType = DataTypes.DoubleType
      case "float" => dataType = DataTypes.FloatType
      case "short" => dataType = DataTypes.ShortType
      case "date" => dataType = DataTypes.DateType
      case "timestamp" => dataType = DataTypes.TimestampType
      case "boolean" => dataType = DataTypes.BooleanType
      case "binary" => dataType = DataTypes.BinaryType
      case "byte" => dataType = DataTypes.ByteType
      case _ => throw new RuntimeException("")
    }
    dataType
  }
}
