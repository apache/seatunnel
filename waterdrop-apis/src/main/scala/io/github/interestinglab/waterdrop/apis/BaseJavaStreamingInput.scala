package io.github.interestinglab.waterdrop.apis

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * A Java-friendly version of [[BaseStreamingInput]] that returns
 * [[BaseJavaStreamingInput]]s and works with JavaStreamingContext, JavaRDD instead of Scala ones.
 * */
abstract class BaseJavaStreamingInput[T] extends BaseStreamingInput[T] {

  def rdd2dataset(spark: SparkSession, rdd: RDD[T]): Dataset[Row] = {
    javaRdd2dataset(spark, rdd.toJavaRDD())
  }

  def getDStream(ssc: StreamingContext): DStream[T] = {
    getJavaDstream(new JavaStreamingContext(ssc)).dstream
  }

  /**
   * Create spark javaDStream from data source, you can specify type parameter.
   * */
  def getJavaDstream(jssc: JavaStreamingContext): JavaDStream[T]

  /**
   * This must be implemented to convert JavaRDD[T] to Dataset[Row] for later processing
   * */
  def javaRdd2dataset(sparkSession: SparkSession, javaRDD: JavaRDD[T]): Dataset[Row]
}
