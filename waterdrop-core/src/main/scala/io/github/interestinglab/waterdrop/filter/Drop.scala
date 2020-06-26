package io.github.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.not

import scala.util.control.NonFatal

class Drop extends BaseFilter {

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
    val sparkSession = SparkSession.builder().getOrCreate()
    conf.hasPath("condition") match {
      case true => {
        try {
          sparkSession.sessionState.sqlParser.parseExpression(conf.getString("condition"))
          (true, "")
        } catch {
          case parseEx: ParseException =>
            (false, "failed to parse [condition], caught exception: " + parseEx.getMessage())
          case NonFatal(e) => (false, e.getMessage)
        }
      }
      case false => (false, "please specify [condition]")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val conditionExpr = conf.getString("condition")
    df.filter(not(new Column(spark.sessionState.sqlParser.parseExpression(conditionExpr))))
  }
}
