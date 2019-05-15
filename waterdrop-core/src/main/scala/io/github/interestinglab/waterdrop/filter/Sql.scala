package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Sql extends BaseFilter {

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
    conf.hasPath("sql") match {
      case true => (true, "")
      // case true => checkSQLSyntax(conf.getString("sql"))
      case false => (false, "please specify parameter [sql]")
    }
  }

  private def checkSQLSyntax(sql: String): (Boolean, String) = {
    val sparkSession = SparkSession.builder.getOrCreate
    val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(sql)

    if (!logicalPlan.analyzed) {
      val logicPlanStr = logicalPlan.toString
      logicPlanStr.toLowerCase.contains("unresolvedrelation") match {
        case true => (true, "")
        case false => {
          val msg = "config[sql] cannot be passed through sql parser, sql[" + sql + "], logicPlan: \n" + logicPlanStr
          (false, msg)
        }
      }
    } else {
      (true, "")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    this.conf.hasPath("table_name") && StringUtils.isNotBlank(this.conf.getString("table_name")) match {
      case true => df.createOrReplaceTempView(this.conf.getString("table_name"))
      case false => {}
    }

    spark.sql(conf.getString("sql"))
  }
}
