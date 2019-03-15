package io.github.interestinglab.waterdrop.output.structuredstreaming

import java.sql.Timestamp
import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.{BaseOutput, BaseStructuredStreamingOutput}
import io.github.interestinglab.waterdrop.entity.OpentsdbCallBack
import io.github.interestinglab.waterdrop.utils.HttpClientService
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataType, TimestampType}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}

class Opentsdb extends BaseStructuredStreamingOutput {

  var config: Config = ConfigFactory.empty()

  var options = new collection.mutable.HashMap[String, String]

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("postUrl") && !config.getString("postUrl").trim.isEmpty &&
    config.hasPath("metric_name") && !config.getString("metric_name").trim.isEmpty match {
      case true => {
        (true, "")
      }
      case false => (false, "[postUrl] and [metric_name] must not be null")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "dimensions" -> util.Arrays.asList(),
        "measures" -> util.Arrays.asList(),
        "timestamp" -> "receive_time",
        "streaming_output_mode" -> "append",
        "triggerMode" -> "default"
      )
    )
    config = config.withFallback(defaultConfig)

  }

  override def process(df: Dataset[Row]): DataStreamWriter[Row] ={

    val triggerMode = config.getString("triggerMode")

    var writer = df.writeStream
      .outputMode(config.getString("streaming_output_mode"))
      .foreach(this)
      .options(options)

    writer = StructuredUtils.setCheckpointLocation(writer, config)

    triggerMode match {
      case "default" => writer
      case "ProcessingTime" => writer.trigger(Trigger.ProcessingTime(config.getString("interval")))
      case "OneTime" => writer.trigger(Trigger.Once())
      case "Continuous" => writer.trigger(Trigger.Continuous(config.getString("interval")))
    }
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(row: Row): Unit = {

    val postBody = buildPostParam(row)

    val callBack = new OpentsdbCallBack(postBody)

    println("[PostBody] : " + postBody)

    HttpClientService.execAsyncPost(config.getString("postUrl"), postBody, callBack)

  }


  override def close(errorOrNull: Throwable): Unit = {

  }

  def buildPostParam(row: Row): String = {

    val map = row2Map(row)
    val postBody = map2Dbentity(map)
    postBody
  }


  /**
   * convert map to row that opentsdb required
   * @param map
   * @return
   */
  def map2Dbentity(map : Map[String,Any]):String = {
    var list : JSONArray = new JSONArray()
    //获取写入Opentsdb的MetricName
    val metric_name = config.getString("metric_name")

    val timestamp = config.getString("timestamp")

    val dimensions = config.getStringList("dimensions")
    val dimensionsMap = map.filterKeys(key => {
      dimensions.contains(key)
    }).asJava

    //按照度量字段信息，将原始数据变成多行数据
    for( measure <- config.getStringList("measures").asScala){
      //添加度量信息
      if(map.contains(measure.toLowerCase())){
        val obj = new JSONObject()
        obj.put("metric",metric_name)
        obj.put("timestamp",map.get(timestamp).get)
        //添加维度信息
        val tagObj = new JSONObject()
        tagObj.putAll(dimensionsMap)
        tagObj.put("stat_group",measure)
        obj.put("tags",tagObj)
        //添加值
        obj.put("value",map.get(measure).get)
        list.add(obj)
      }
    }
    list.toString
  }

    /**
     * convert row in dataframe to map
     * */
  def row2Map(row: Row): Map[String, Any] = {

    var map: Map[String, Any] = Map()
    val schema = row.schema
    for (filed <- schema.fields) {
      breakable {
        val index = schema.fieldIndex(filed.name)
        val value = row.get(index)

        //如果是复杂类型，暂时不做处理
        if (!filed.dataType.isInstanceOf[DataType]) {
          break
        }
        if (filed.dataType == TimestampType) {
          val string = value.asInstanceOf[Timestamp].getTime / 1000
          map = map + ((filed.name.toLowerCase(), string))
        } else {
          val formatValue = replaceIllegalLetter(value.toString)
          map = map + ((filed.name.toLowerCase(), formatValue))
        }
      }
    }
    map
  }

  /**
   * exclude the illegeal char that not supported by opentsdb
   */

  def replaceIllegalLetter(value : String): String ={

    val expression = "[^a-zA-Z0-9-._/\\u4E00-\\u9FA5]"
    val ret = value.replaceAll(expression,"_")
    ret
  }


}
