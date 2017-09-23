package org.interestinglab.waterdrop.filter

import scala.collection.JavaConverters._
import java.util.ServiceLoader

import org.apache.spark.sql.SparkSession

/**
 * Created by gaoyingju on 24/09/2017.
 */
object UdfRegister {

  def findAndRegisterUdfs(spark: SparkSession): Unit = {

    println("find and register UDFs & UDAFs")

    var udfCount = 0
    var udafCount = 0
    val services = (ServiceLoader load classOf[BaseFilter]).asScala
    services.foreach(f => {

      f.getUdfList()
        .foreach(udf => {
          spark.udf.register(udf._1, udf._2)
          udfCount += 1
        })

      f.getUdafList()
        .foreach(udaf => {
          spark.udf.register(udaf._1, udaf._2)
          udafCount += 1
        })
    })

    println("found and registered UDFs count[" + udfCount + "], UDAFs count[" + udafCount + "]")
  }
}
