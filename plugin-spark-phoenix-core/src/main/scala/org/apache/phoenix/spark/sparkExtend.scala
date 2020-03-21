package org.apache.phoenix.spark

import org.apache.spark.sql.{DataFrame, SQLContext}

object sparkExtend {

  implicit def toSparkSqlContextFunctions2(sqlContext: SQLContext): SparkSqlContextFunctions2 = {
    new SparkSqlContextFunctions2(sqlContext)
  }

  implicit def toDataFrameFunctions(data: DataFrame): DataFrameFunctions2 = {
    new DataFrameFunctions2(data)
  }

}
