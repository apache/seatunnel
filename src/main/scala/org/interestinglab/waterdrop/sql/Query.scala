package org.interestinglab.waterdrop.sql

import com.typesafe.config.Config
import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext


class Query(var conf: Config) extends BaseSQL(conf){

    var sqlContext : SparkSession = _

    /**
     *  return true and empty string if config is valid, return false and error message if config is invalid
     * */
    def checkConfig() : (Boolean, String) = (true, "")

    def prepare(ssc : StreamingContext) : Unit = {

        this.sqlContext = SQLContextFactory.getInstance()
    }

    def query(df : DataFrame) : DataFrame = {

        // TODO : when to drop registered table ?
        df.createOrReplaceTempView(this.conf.getString("table_name"))
        this.sqlContext.sql(this.conf.getString("sql"))
    }
}
