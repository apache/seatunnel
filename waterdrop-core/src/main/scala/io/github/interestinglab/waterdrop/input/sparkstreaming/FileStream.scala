package io.github.interestinglab.waterdrop.input.sparkstreaming

import com.databricks.spark.xml.{XmlInputFormat, XmlReader}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStreamingInput
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

class FileStream extends BaseStreamingInput[String] {

  var config: Config = ConfigFactory.empty()

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
    config.hasPath("path") match {
      case true => {
        if (config.hasPath("format") && config.getString("format") == "xml" && !config.hasPath("rowTag")) {
          (false, "please specify [rowTag] as non-empty string when format is <xml>")
        } else {
          val dir = config.getString("path")
          val path = new org.apache.hadoop.fs.Path(dir)
          Option(path.toUri.getScheme) match {
            case None => (true, "")
            case Some(schema) => (true, "")
            case _ =>
              (
                false,
                "unsupported schema, please set the following allowed schemas: file://, for example: file:///var/log")
          }
        }
      }
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "format" -> "text"
      )
    )

    config = config.withFallback(defaultConfig)
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }

    path
  }

  protected def streamFileReader(ssc: StreamingContext, path: String): DStream[String] = {

    if (this.config.getString("format") == "xml") {
      val rowTag = this.config.getString("rowTag")
      ssc.sparkContext.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, s"<$rowTag>")
      ssc.sparkContext.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, s"</$rowTag>")
      ssc.fileStream[LongWritable, Text, XmlInputFormat](path).map { case (_, text) => text.toString }
    } else {
      ssc.textFileStream(path)
    }
  }

  override def getDStream(ssc: StreamingContext): DStream[String] = {

    streamFileReader(ssc, buildPathWithDefaultSchema(config.getString("path"), "file://"))
  }

  override def rdd2dataset(spark: SparkSession, rdd: RDD[String]): Dataset[Row] = {

    if (this.config.getString("format") == "xml") {
      val reader = new XmlReader()
      reader.withRowTag(config.getString("rowTag"))
      reader.xmlRdd(spark, rdd)
    } else {
      val rowsRDD = rdd.map(element => {
        RowFactory.create(element)
      })
      val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
      spark.createDataFrame(rowsRDD, schema)
    }
  }
}
