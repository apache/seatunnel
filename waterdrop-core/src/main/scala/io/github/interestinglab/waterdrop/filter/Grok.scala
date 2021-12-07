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

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.Common
import io.github.interestinglab.waterdrop.core.RowConstant
import io.thekraken.grok.api.{Grok => GrokLib}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConverters._

class Grok extends BaseFilter {

  val grok = new GrokLib()

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("pattern") match {
      case true => (true, "")
      case false => (false, "please specify [pattern]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    logInfo("grok plugin dir relative path: " + Common.pluginDir("grok"))
    logInfo("grok plugin dir absolute path: " + new File(Common.pluginDir("grok").toString).getAbsolutePath)
    logInfo(
      "list files of grok plugin dir: " + new File(Common.pluginFilesDir("grok").toString)
        .listFiles()
        .foldRight("")((f, b) => b + ", " + f.getName))

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "patterns_dir" -> Paths
          .get(Common.pluginFilesDir("grok").toString, "grok-patterns")
          .toString,
        "named_captures_only" -> true,
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT
      ).asJava
    )
    conf = conf.withFallback(defaultConfig)

    // compile predefined patterns
    getListOfFiles(conf.getString("patterns_dir")).foreach(f => {
      grok.addPatternFromFile(f.getAbsolutePath)
    })

    grok.compile(conf.getString("pattern"), true)

  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val grokUDF = udf((str: String) => grokMatch(str))
    val keys = getKeysOfPattern(conf.getString("pattern"))
    conf.getString("target_field") match {
      case RowConstant.ROOT => {
        var tmpDf = df.withColumn(RowConstant.TMP, grokUDF(col(conf.getString("source_field"))))
        while (keys.hasNext) {
          val field = keys.next()
          tmpDf = tmpDf.withColumn(field, col(RowConstant.TMP)(field))
        }
        tmpDf.drop(RowConstant.TMP)
      }
      case targetField => {
        df.withColumn(targetField, grokUDF(col(conf.getString("source_field"))))
      }
    }
  }

  private def grokMatch(str: String): scala.collection.Map[String, String] = {
    val gm = grok.`match`(str)
    gm.captures()
    gm.toMap.asScala.mapValues(_.asInstanceOf[String])
  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  private def getKeysOfPattern(pattern: String): util.Iterator[String] = {
    grok.getNamedRegexCollection.values().iterator()
  }
}
