package org.apache.seatunnel.spark.webhook.source

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.apache.spark.sql.execution.streaming.MemoryStream

class HttpPushServlet(stream: MemoryStream[HttpData]) extends HttpServlet {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val resBody = Source.fromInputStream(req.getInputStream).mkString
    val timestamp = new java.sql.Timestamp(System.currentTimeMillis())
    stream.addData(HttpData(resBody, timestamp))

    resp.setContentType("application/json;charset=utf-8")
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.write("""{"success": true}""")
  }

}
