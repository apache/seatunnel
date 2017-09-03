package org.interestinglab.waterdrop

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.interestinglab.waterdrop.config.ConfigBuilder

object WaterdropMain {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf()
    val duration = 15;
    val ssc = new StreamingContext(sparkConf, Seconds(duration))

    val configBuilder = ConfigBuilder(conf)
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
      i.prepare(ssc)
    }

    for (o <- outputs) {
      o.prepare(ssc)
    }

    //for (f <- filters) {
    //  f.prepare(ssc)
    //}
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

      val sqlContext = SparkSession.builder().getOrCreate()

      val schema = StructType(Array(StructField("raw_message", StringType)))
      var df = sqlContext.createDataFrame(rowsRDD, schema)

      for (f <- filters) {
        f.prepare(sqlContext)
      }

      for(f <- filters) {
        df = f.filter(df, sqlContext)
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
