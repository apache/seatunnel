package org.interestinglab.waterdrop

import org.interestinglab.waterdrop.core.{Event, Plugin}
import org.interestinglab.waterdrop.sql.SQLContextFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.interestinglab.waterdrop.core.Event
import org.interestinglab.waterdrop.sql.SQLContextFactory
import org.interestinglab.waterdrop.config.ConfigBuilder
import org.interestinglab.waterdrop.core.Event
import org.interestinglab.waterdrop.sql.SQLContextFactory


object WaterdropMain {

    def main(args : Array[String]) {
        val conf = ConfigFactory.load()

        val sparkConf = new SparkConf()
        val ssc = new StreamingContext(sparkConf, Seconds(15))

        val configBuilder = ConfigBuilder(conf)
        val inputs = configBuilder.createInputs()
        val outputs = configBuilder.createOutputs()
        val filters = configBuilder.createFilters()
        val sqls = configBuilder.createSQLs()

        var configValid = true
        val plugins = inputs ::: filters ::: sqls ::: outputs
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

        for (f <- filters) {
            f.prepare(ssc)
        }

        val dstream = inputs.head.getDstream.mapPartitions{ iter =>
            // val eventIter = iter.map(r => Event(Map("raw_message" -> r)))
            val eventIter = iter.map(r => Event(Map("raw_message" -> r._2))) // kafka input related !!!! maybe not common used
            var events = eventIter.toList

            for (f <- filters) {
                val (processedEvents, isSuccess) = f.filter(events)
                events = processedEvents
            }

            events.iterator
        }

        if (conf.hasPath("sql")) {

            val sqlConf = conf.getConfig("sql")
            // For now we only use first query config of "sql"
            val queryConf = sqlConf.getConfigList("query")

            val jsonStream = dstream.map(_.toJSON())

            jsonStream.foreachRDD { jsonRDD =>

                // DataFrame output or DataFrame to rdd for output
                //     * DataFrame can save as Parquet, orc, json, avro(installed plugin) to hdfs directly, dataframe.write.json(path)
                //     * DataFrame can be converted to json RDD[String] directly by DataFrame.toJSON
                //     * DataFrame can be converted to RDD or you can do RDD actions on dataframe

                val sqlContext = SQLContextFactory.getInstance(jsonRDD.sparkContext)
                // import sqlContext.implicits._
                var df = sqlContext.read.json(jsonRDD)

                for (query <- sqls) {
                    df = query.query(df)
                }

                val eventRDD = df.toJSON.map(Event(_))
                inputs.head.beforeOutput
                eventRDD.foreachPartition { partitionOfRecords =>
                    outputs.head.process(partitionOfRecords)
                }
                inputs.head.afterOutput

                // TODO: Using Row and Schema to implement Event to avoid overhead of 2 conversion (before sql, Event -> json -> Row, after sql Row -> json -> Event)
                // spark source code : class GenericRow(protected[sql] val values: Array[Any]) extends Row
                // Spark 2.0+ probably can do this internally, streaming on spark sql
                // http://spark.apache.org/docs/1.6.3/api/scala/index.html#org.apache.spark.sql.Row
                // http://stackoverflow.com/questions/33007840/spark-extracting-values-from-a-row
                // http://stackoverflow.com/questions/34025528/nested-json-in-spark
                // http://bigdatums.net/2016/02/12/how-to-extract-nested-json-data-in-spark/
            }
        }
        else {

            dstream.foreachRDD { rdd =>
                inputs.head.beforeOutput

                rdd.foreachPartition { partitionOfRecords =>
                    // inputs[0].beforeOutput()
                    outputs.head.process(partitionOfRecords)
                    // inputs[0].afterOutput()
                }

                inputs.head.afterOutput
            }
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
