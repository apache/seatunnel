package org.interestinglab.waterdrop.filter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructField

import scala.collection.mutable.HashMap

/**
 * Created by huochen on 2017/8/17.
 */
class FilterTest(var conf : Config) extends BaseFilter(conf) {
  def checkConfig() : (Boolean, String) = {
    // TODO
    (true, "")
  }


  def process(df : DataFrame) : DataFrame = {

    import org.apache.spark.sql.types._
    // var isSuccess = List[Boolean]()
    val sqlContext = this.sqlContext

    import sqlContext.implicits._

    val rows = df.rdd.map { r =>
      Row.fromSeq(r.toSeq ++ udfFunc(r.getAs[String]("raw_message")))
    }

    val schema = StructType(df.schema.fields ++ structField())


    sqlContext.createDataFrame(rows, schema)
    val func = udf((s:String) => s.length)
    df.withColumn("tmp", func($"raw_message"))


  }

  def udfFunc(str : String) : Seq[Any] = {

    //val s = str.substring(0, 5)

    Seq(str.trim, str.length)
  }



  def structField() : Array[StructField] = {
    import org.apache.spark.sql.types._

    Array(StructField("prefix", StringType),
      StructField("prelen", IntegerType))
  }

}
