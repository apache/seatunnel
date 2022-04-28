/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.spark.webhook.source

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.spark_project.jetty.server.Server
import org.spark_project.jetty.servlet.{ServletContextHandler, ServletHolder}

class JettyServer(port: Int = 9999, baseUrl: String = "/") {

  // Create server
  var server: Server = new Server(port)

  /**
   * Starts an HTTP server and initializes memory stream.
   * As requests are made to given http endpoint, it puts data on memory stream.
   * Returns a streaming DF created off of memory stream.
   *
   * @param sqlContext
   * @return
   */
  def toDF(implicit sqlContext: SQLContext): DataFrame = {

    // Create a memory Stream
    implicit val enc: Encoder[HttpData] = Encoders.product[HttpData]
    val stream = MemoryStream[HttpData]

    var context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setContextPath("/")
    server.setHandler(context)

    val servlet = new HttpPushServlet(stream)
    context.addServlet(new ServletHolder(servlet), baseUrl)

    // Start server and return streaming DF
    server.start()
    stream.toDF()
  }

  def stop(): Unit = {
    server.stop()
  }
}
