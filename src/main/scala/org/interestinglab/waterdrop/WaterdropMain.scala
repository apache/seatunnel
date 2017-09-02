package org.interestinglab.waterdrop

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.interestinglab.waterdrop.config.ConfigBuilder
import org.interestinglab.waterdrop.core.Event
import org.interestinglab.waterdrop.serializer.Json

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

    val dstream = inputs.head.getDstream.mapPartitions { iter =>
      // val eventIter = iter.map(r => Event(Map("raw_message" -> r)))
      val eventIter = iter.map(r =>
        Event(Map("raw_message" -> Event.toJValue(r._2)))) // kafka input related !!!! maybe not common used
      val events = eventIter.toList

      events.iterator
    }

    val jsonStream = dstream.map(_.toJSON())

    jsonStream.foreachRDD { jsonRDD =>
      val sqlContext = SparkSession.builder().getOrCreate()
      var df = sqlContext.read.json(jsonRDD)

      for (f <- filters) {
        f.prepare(sqlContext)
      }

      for(f <- filters) {
        df = f.filter(df, sqlContext)
        df.show()
      }

      val eventRDD = df.toJSON.rdd.map { eventStr =>
        Json.deserialize(eventStr)
      }

      inputs.head.beforeOutput
      eventRDD.foreachPartition { partitionOfRecords =>
        outputs.head.process(partitionOfRecords)
      }
      inputs.head.afterOutput

    }


    ssc.start()
    ssc.awaitTermination()
  }

  // def doFilter(iter : Iterator[String]) : Iterator[String] = {
  //     //a.mapPartitions(iter => iter.map(r => r *2))
  //     //for (rec <- records) {}
  //
  //     for
  // }
}
