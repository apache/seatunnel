package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.not
import scala.util.control.NonFatal

class Drop(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
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

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    val conditionExpr = conf.getString("condition")
    df.filter(not(new Column(spark.sessionState.sqlParser.parseExpression(conditionExpr))))
  }
}
