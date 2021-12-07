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
package io.github.interestinglab.waterdrop.filter

import java.io.File
import java.nio.file.Paths
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.ql.util.express.{DefaultContext, ExpressRunner}
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.Common
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    conf.hasPath("script_name") && !"".equals(conf.getString("script_name")) match {
      case true => (true, "")
      case false => (false, "please specify [script_name] ")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val pathStr = Paths.get(Common.pluginFilesDir("script").toString).toString
    val name = conf.getString("script_name")

    getListOfFiles(pathStr).foreach(f =>
      f.getName.equals(name) match {
        case true => ql = Source.fromFile(f.getAbsolutePath).mkString
        case false =>
    })

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "object_name" -> "event",
        "errorList" -> false,
        "isCache" -> false,
        "isTrace" -> false,
        "isPrecise" -> false
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  //TODO 多次json序列化带来的性能问题
  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val json = df.toJSON

    val partitions = json
      .mapPartitions(x => {

        /**
         * isPrecise是否需要高精度的计算，
         * isTrace是否输出所有的跟踪信息，同时还需要log级别是DEBUG级
         */
        val runner = new ExpressRunner(conf.getBoolean("isPrecise"), conf.getBoolean("isTrace"))
        val context = new DefaultContext[String, AnyRef]
        val list = new util.ArrayList[String]
        while (x.hasNext) {
          val value = x.next()
          val jsonObject = JSON.parseObject(value)
          val errorList = conf.getBoolean("errorList") match {
            case true => new util.ArrayList[String]
            case false => null
          }

          context.put(conf.getString("object_name"), jsonObject)

          /**
           * ql 程序文本
           * context 执行上下文
           * errorList 输出的错误信息List
           * isCache 是否使用Cache中的指令集
           */
          val execute = runner.execute(ql, context, errorList, conf.getBoolean("isCache"), conf.getBoolean("isTrace"))

          list.add(JSON.toJSONString(execute, SerializerFeature.WriteMapNullValue))
        }
        list.iterator()
      })
      .as[String]

    spark.read.json(partitions)

  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
