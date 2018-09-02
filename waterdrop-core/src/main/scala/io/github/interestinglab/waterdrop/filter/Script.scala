package io.github.interestinglab.waterdrop.filter

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.ql.util.express.{DefaultContext, ExpressRunner}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.io.Source

class Script extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  var ql: String = _

  override def setConfig(config: Config): Unit = {
    this.conf = config
  }


  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("script_path") && !"".equals(conf.getString("script_path")) match {
      case true => (true, "")
      case false => (false, "please specify [script_path] ")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val path = conf.getString("script_path")
    ql = Source.fromFile(path).mkString
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "json_name" -> "value",
        "errorList" -> false,
        "isCache" -> false,
        "isTrace" -> false,
        "isShortCircuit" -> true,
        "isPrecise" -> false
      )
    )
    conf = conf.withFallback(defaultConfig)
  }


  //TODO 多次json序列化带来的性能问题
  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val json = df.toJSON

    val partitions = json.mapPartitions(x => {
      val runner = new ExpressRunner
      val context = new DefaultContext[String, AnyRef]
      val list = new util.ArrayList[String]
      while (x.hasNext) {
        val value = x.next()
        val jsonObject = JSON.parseObject(value)
        val errorList = conf.getBoolean("errorList") match {
          case true => new util.ArrayList[String]
          case false => null
        }

        context.put(conf.getString("json_name"), jsonObject)

        val execute = runner.execute(ql, context
          , errorList, conf.getBoolean("isCache"), conf.getBoolean("isTrace"))

        list.add(JSON.toJSONString(execute, SerializerFeature.WriteMapNullValue))
      }
      list.iterator()
    }).as[String]

    spark.read.json(partitions)

  }
}
