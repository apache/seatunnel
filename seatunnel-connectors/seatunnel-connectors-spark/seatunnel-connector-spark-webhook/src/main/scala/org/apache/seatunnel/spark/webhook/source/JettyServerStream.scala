package org.apache.seatunnel.spark.webhook.source

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SQLContext}
import org.spark_project.jetty.server.Server
import org.spark_project.jetty.servlet.{ServletContextHandler, ServletHolder}

class JettyServerStream(port: Int = 9999, baseUrl: String = "/") {
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

    // Create server
    var server = new Server(port)
    var context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setContextPath("/")
    server.setHandler(context)

    val servlet = new HttpPushServlet(stream)
    context.addServlet(new ServletHolder(servlet), baseUrl)

    // Start server and return streaming DF
    server.start()
    stream.toDF()
  }
}
