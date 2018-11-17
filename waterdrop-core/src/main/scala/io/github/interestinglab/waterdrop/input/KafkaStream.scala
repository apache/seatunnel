package io.github.interestinglab.waterdrop.input

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStreamingInput
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._

class KafkaStream extends BaseStreamingInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }


  override def getConfig(): Config = {
    this.config
  }

  // kafka consumer configuration : http://kafka.apache.org/documentation.html#oldconsumerconfigs
  val consumerPrefix = "consumer"

  var offsetRanges = Array[OffsetRange]()

  var km: KafkaManager = _

  override def checkConfig(): (Boolean, String) = {

    config.hasPath("topics") match {
      case true => {
        val consumerConfig = config.getConfig(consumerPrefix)
        consumerConfig.hasPath("zookeeper.connect") &&
          !consumerConfig.getString("zookeeper.connect").trim.isEmpty &&
          consumerConfig.hasPath("group.id") &&
          !consumerConfig.getString("group.id").trim.isEmpty match {
          case true => (true, "")
          case false =>
            (false, "please specify [consumer.zookeeper.connect] and [consumer.group.id] as non-empty string")
        }
      }
      case false => (false, "please specify [topics] as non-empty string, multiple topics separated by \",\"")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        consumerPrefix + ".key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        consumerPrefix + ".value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )
    )

    config = config.withFallback(defaultConfig)
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

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

    val topics = config.getString("topics").split(",").toSet
    km = new KafkaManager(kafkaParams)
    val fromOffsets = km.setOrUpdateOffsets(topics, consumerConfig.getString("group.id"))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe(topics, kafkaParams, fromOffsets))

    inputDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(record => {
        val topic = record.topic()
        val value = record.value()
        (topic, value)
      })
    }
  }

  override def afterOutput {
    // update offset after output
    km.updateZKOffsetsFromoffsetRanges(offsetRanges)
  }
}

class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable with ZkSerializer {

  def setOrUpdateOffsets(topics: Set[String], groupId: String): Map[TopicPartition, Long] = {

    val zk = kafkaParams.get("zookeeper.connect") match {
      case Some(v) => v
      case None => throw new ConfigRuntimeException("kafkaStream config consumer.zookeeper.connect can not be empty!")
    }
    var fromOffsets: Map[TopicPartition, Long] = Map()
    val zkClient = new ZkClient(zk)
    zkClient.setZkSerializer(this)
    topics.foreach(topic => {
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      val zkPath = topicDirs.consumerOffsetDir
      val children = zkClient.countChildren(zkPath)
      if (children > 0) {
        for (i <- 0 until children) {
          val partitionOffset = zkClient.readData[String](s"${zkPath}/${i}")
          val tp = new TopicPartition(topic, i)
          fromOffsets += (tp -> partitionOffset.toLong)
        }
      }
    })
    fromOffsets
  }

  def updateZKOffsetsFromoffsetRanges(offsetRanges: Array[OffsetRange]): Unit = {

    val zk = kafkaParams.get("zookeeper.connect") match {
      case Some(v) => v
      case None => throw new ConfigRuntimeException("kafkaStream config consumer.zookeeper.connect can not be empty!")
    }
    val groupId = kafkaParams.get("group.id") match {
      case Some(v) => v
      case None => throw new ConfigRuntimeException("kafkaStream config consumer.group.id can not be empty!")
    }
    val sessionTimeOut = 180000
    val connectionTimeOut = 5000
    val zkUtils = ZkUtils.apply(zk, connectionTimeOut, sessionTimeOut, JaasUtils.isZkSecurityEnabled)
    for (offsets <- offsetRanges) {
      val topic = offsets.topic
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      val zkPath = topicDirs.consumerOffsetDir
      val newZkPath = s"${zkPath}/${offsets.partition}"
      zkUtils.updatePersistentPath(newZkPath, String.valueOf(offsets.untilOffset))
      println(s"complete consume topic: $topic partition: ${offsets.partition} from ${offsets.fromOffset} until ${offsets.untilOffset}")
    }
  }

  override def serialize(data: scala.Any): Array[Byte] = String.valueOf(data).getBytes("utf-8")

  override def deserialize(bytes: Array[Byte]): AnyRef = new String(bytes, "utf-8")
}
