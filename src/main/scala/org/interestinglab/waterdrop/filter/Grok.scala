package org.interestinglab.waterdrop.filter

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import io.thekraken.grok.api.{Grok => GrokLib}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.{col, udf}
import org.interestinglab.waterdrop.config.Common

import scala.collection.JavaConverters._

class Grok(var conf: Config) extends BaseFilter(conf) {

  val grok = GrokLib.EMPTY

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("pattern") match {
      case true => (true, "")
      case false => (false, "please specify [pattern]")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    // TODO: 如何拿到目录下的所有文件, 而不是拿特定的文件，还包括多级目录结构的问题。
    val patternPath = "grok-patterns"
    logWarning("grok pattern path: " + patternPath)
    logWarning("SparkFiles.get : " + SparkFiles.get(patternPath))
    logWarning("SparkFiles.getRootDirectory : " + SparkFiles.getRootDirectory())
    new File(SparkFiles.getRootDirectory()).list.foreach(f => {
      logWarning("list files: " + f)
    })

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "patterns_dir" -> Paths
          .get(Common.pluginFilesDir("grok").toString, "grok-patterns")
          .toString,
        "named_captures_only" -> true,
        "source_field" -> "raw_message",
        "target_field" -> Json.ROOT
      ).asJava
    )
    conf = conf.withFallback(defaultConfig)

    // compile predefined patterns
    // TODO: 是怎么找到这个文件的？？在hdfs上，在local root dir ?? 但是为什么list不出来 !!!
    grok.addPatternFromFile(patternPath)

    grok.compile(conf.getString("pattern"), true)

    // TODO: get fixed field list to insert fields to ROOT
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    val grokUDF = udf((str: String) => grokMatch(str))
    conf.getString("target_field") match {
      case Json.ROOT => df // TODO
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
      // d.listFiles.filter(_.isFile).toList
      d.listFiles.toList
    } else {
      List[File]()
    }
  }
}
