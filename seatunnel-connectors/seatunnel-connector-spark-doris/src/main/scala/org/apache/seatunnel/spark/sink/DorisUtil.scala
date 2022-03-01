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

package org.apache.seatunnel.spark.sink

import org.apache.commons.net.util.Base64
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, DefaultConnectionKeepAliveStrategy, DefaultRedirectStrategy, HttpClientBuilder}
import org.apache.log4j.Logger
import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.{Charset, StandardCharsets}

import scala.util.{Failure, Success, Try}

object DorisUtil extends Serializable {

  private val LOG = Logger.getLogger(this.getClass)

  def createClient: CloseableHttpClient =
    HttpClientBuilder.create()
      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy)
      .setRedirectStrategy(new DefaultRedirectStrategy() {
        override def isRedirectable(method: String): Boolean = {
          super.isRedirectable(method)
          true
        }
      }).build()

  def streamLoad(httpclient: CloseableHttpClient,
                 headers: Map[String, String],
                 messages: String,
                 api: String,
                 user: String,
                 password: String): (Boolean, CloseableHttpClient, CloseableHttpResponse) = {

    var response: CloseableHttpResponse = null
    var status = true
    try {
      val httpPut = new HttpPut(api)
      val requestConfig = RequestConfig.custom()
        .setAuthenticationEnabled(true)
        .setCircularRedirectsAllowed(true)
        .setRedirectsEnabled(true)
        .setRelativeRedirectsAllowed(true)
        .setExpectContinueEnabled(true)
        .setConnectTimeout(Config.TIMEOUT).setConnectionRequestTimeout(Config.TIMEOUT)
        .setSocketTimeout(Config.TIMEOUT).build()
      httpPut.setConfig(requestConfig)
      httpPut.setHeader(HttpHeaders.EXPECT, "100-continue")
      httpPut.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(user, password))
      if (headers != null && headers.nonEmpty) {
        headers.foreach(entry => {
          httpPut.setHeader(entry._1, entry._2)
        })
      }
      val content = new StringEntity(messages, Charset.forName(Config.CHARSET))
      content.setContentType(Config.CONTENT_TYPE)
      content.setContentEncoding(Config.CHARSET)
      httpPut.setEntity(content)
      response = httpclient.execute(httpPut)
      val bufferReader = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
      val stringBuffer = new StringBuffer()
      var str = ""
      while (str != null) {
        stringBuffer.append(str.trim)
        str = bufferReader.readLine()
      }
      LOG.info(
        s"""
           |Batch Messages Response:
           |${stringBuffer.toString}
           |""".stripMargin)
    } catch {
      case _: Exception => status = false
        (status, httpclient, response)
    }
    (status, httpclient, response)
  }


  def basicAuthHeader(username: String, password: String): String = {
    val tobeEncode: String = username + ":" + password
    val encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8))
    val res = "Basic " + new String(encoded)
    res
  }
}

class DorisUtil(httpHeader: Map[String, String], apiUrl: String, user: String, password: String) {
  def saveMessages(messages: String): Unit = {
    val httpClient = DorisUtil.createClient
    val result = Try(DorisUtil.streamLoad(httpClient, httpHeader, messages, apiUrl, user, password))
    result match {
      case Success(_) =>
        httpClient.close()
        result.get._2.close()
      case Failure(var1: Exception) =>
        httpClient.close()
        result.get._2.close()
        throw new RuntimeException(var1.getMessage)
    }
  }
}
