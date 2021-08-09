package io.github.interestinglab.waterdrop.output.batch

import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SparkSession}

import java.util
import scala.collection.JavaConversions._

class Hive extends BaseOutput {

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
    config.hasPath("sql") || (config.hasPath("source_table_name") && config.hasPath("result_table_name")) match {
      case true =>  (true ,"")
      case false => (false,"请使用 sql 或者  (source_table_name 和 result_table_name) ")
    }
  }

  override def prepare(spark: SparkSession): Unit = {

  }

  override def process(df: Dataset[Row]): Unit = {
    val sparkSession = df.sparkSession
    config.hasPath("sql") match {
      case true => {
        val sql = config.getString("sql")
        sparkSession.sql(sql)
      }
      case _ =>{
        val sourceTableName = config.getString("source_table_name")
        val resultTableName = config.getString("result_table_name")
        val sinkColumns = config.hasPath("sink_columns") match {
          case true => config.getString("sink_columns")
          case false => "*"
        }
        val sinkFrame = sparkSession.sql(s"select $sinkColumns from $sourceTableName")
        val frameWriter: DataFrameWriter[Row] = config.hasPath("save_mode") match {
          case true => sinkFrame.write.mode(config.getString("save_mode"))
          case _ => sinkFrame.write
        }
        config.hasPath("partition_by") match {
          case true =>
            val partitionList: util.List[String] = config.getStringList("partition_by")
            frameWriter.partitionBy(partitionList:_*).saveAsTable(resultTableName)
          case _ => frameWriter.saveAsTable(resultTableName)
        }
      }
    }

  }

}
