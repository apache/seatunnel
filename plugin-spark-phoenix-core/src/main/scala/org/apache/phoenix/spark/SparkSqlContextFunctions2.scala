package org.apache.phoenix.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SQLContext}

final class SparkSqlContextFunctions2(@transient val sqlContext: SQLContext) extends Serializable {

  def phoenixTableAsDataFrame(table: String, columns: Seq[String],
                              predicate: Option[String] = None,
                              zkUrl: Option[String] = None,
                              tenantId: Option[String] = None): DataFrame = {
    val config: Configuration = HBaseConfiguration.create()
    val job: Job = Job.getInstance(config)
    TableMapReduceUtil.initCredentials(job)
    new PhoenixRDD2(sqlContext.sparkContext, table, columns, predicate, zkUrl, job.getCredentials, tenantId = tenantId).toDataFrame(sqlContext)
  }

}
