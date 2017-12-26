package io.github.interestinglab.waterdrop.input

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.SparkException
import org.apache.spark.streaming.dstream.DStream
import com.typesafe.config.Config
import _root_.kafka.message.MessageAndMetadata
import _root_.kafka.common.TopicAndPartition
import io.github.interestinglab.waterdrop.apis.BaseInput

import scala.collection.JavaConversions._

class Kafka(config: Config) extends BaseInput(config) {

  // kafka consumer configuration : http://kafka.apache.org/documentation.html#oldconsumerconfigs
  val consumerPrefix = "consumer"

  var offsetRanges = Array[OffsetRange]()

  var km: KafkaManager = null

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
      case false => (false, "please specify [topics]as non-empty string, multiple topics separated by \",\"")
    }
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

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    val topics = config.getString("topics").split(",").toSet
    km = new KafkaManager(kafkaParams)
    val fromOffsets =
      km.setOrUpdateOffsets(topics, consumerConfig.getString("group.id"))

    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc,
      kafkaParams,
      fromOffsets,
      messageHandler)

    inputDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
  }

  override def afterOutput {
    // update offset after output
    km.updateZKOffsetsFromoffsetRanges(offsetRanges)
  }
}

class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {

  private val kc = new KafkaCluster(kafkaParams)

  def setOrUpdateOffsets(topics: Set[String], groupId: String): Map[TopicAndPartition, Long] = {

    val defaultOff = 10000000 // debug 在偏移的基础上再偏移10亿, 防止因为kafka删除过期log的原因导致读kafka topic出现 kafka.common.OffsetOutOfRangeException

    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft) {
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      }

      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) { // 消费过
        /**
         * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
         * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
         * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
         * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
         * 这时把consumerOffsets更新为earliestLeaderOffsets
         */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft) {
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        }

        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({
          case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if (n < earliestLeaderOffset) {
              println(
                "consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
                  " offsets已经过时，更新为" + earliestLeaderOffset)
              offsets += (tp -> earliestLeaderOffset)
            }
        })
        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else { // 没有消费过
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] =
          null
        if (reset == Some("smallest")) {
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft) {
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          }

          leaderOffsets = leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft) {
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          }

          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map {
          // case (tp, offset) => (tp, offset.offset + defaultOff) // debug, in this debug code, largest will cause out of range offset !!!!

          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })

    val partitionsE = kc.getPartitions(topics)
    if (partitionsE.isLeft) {
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    }

    val partitions = partitionsE.right.get
    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if (consumerOffsetsE.isLeft) {
      throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
    }

    consumerOffsetsE.right.get
  }

  def updateZKOffsetsFromoffsetRanges(offsetRanges: Array[OffsetRange]): Unit = {
    val groupId = kafkaParams.get("group.id").get

    for (offsets <- offsetRanges) {
      val topicAndPartition =
        TopicAndPartition(offsets.topic, offsets.partition)

      println("partition: " + offsets.partition + ", from: " + offsets.fromOffset + ", until: " + offsets.untilOffset)

      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }

}
