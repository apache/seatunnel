package org.interestinglab.waterdrop

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.interestinglab.waterdrop.config.ConfigBuilder
import org.interestinglab.waterdrop.filter.UdfRegister

object WaterdropMain {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val duration = 15;
    val ssc = new StreamingContext(sparkConf, Seconds(duration))
    val sparkSession = SparkSession.builder.config(ssc.sparkContext.getConf).getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    val configBuilder = new ConfigBuilder
    val inputs = configBuilder.createInputs()
    val outputs = configBuilder.createOutputs()
    val filters = configBuilder.createFilters()

    var configValid = true
    val plugins = inputs ::: filters ::: outputs
    for (p <- plugins) {
      val (isValid, msg) = p.checkConfig
      if (!isValid) {
        configValid = false
        printf("Plugin[%s] contains invalid config, error: \n", p.name, msg)
      }
    }

    if (!configValid) {
      System.exit(-1) // invalid configuration
    }

    for (i <- inputs) {
      i.prepare(sparkSession, ssc)
    }

    for (o <- outputs) {
      o.prepare(sparkSession, ssc)
    }

    for (f <- filters) {
      f.prepare(sparkSession, ssc)
    }

    val dStream = inputs.head.getDstream().mapPartitions { partitions =>
      val strIterator = partitions.map(r => r._2)
      val strList = strIterator.toList
      strList.iterator
    }

    dStream.foreachRDD { strRDD =>
      val rowsRDD = strRDD.mapPartitions { partitions =>
        val row = partitions.map(Row(_))
        val rows = row.toList
        rows.iterator
      }

      val spark = SparkSession.builder.config(rowsRDD.sparkContext.getConf).getOrCreate()

      val schema = StructType(Array(StructField("raw_message", StringType)))
      var df = spark.createDataFrame(rowsRDD, schema)

      for (f <- filters) {
        df = f.process(spark, df)
        df.show()
      }

      inputs.head.beforeOutput
      outputs.head.process(df)
      inputs.head.afterOutput

    }

    ssc.start()
    ssc.awaitTermination()
  }
}
