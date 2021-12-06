/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.filter

import scala.collection.JavaConverters._
import java.util.ServiceLoader

import io.github.interestinglab.waterdrop.apis.BaseFilter
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
          val (udfName, udfImpl) = udf
          spark.udf.register(udfName, udfImpl)
          udfCount += 1
        })

      f.getUdafList()
        .foreach(udaf => {
          val (udafName, udafImpl) = udaf
          spark.udf.register(udafName, udafImpl)
          udafCount += 1
        })
    })

    println("found and registered UDFs count[" + udfCount + "], UDAFs count[" + udafCount + "]")
  }
}
