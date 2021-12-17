package io.github.interestinglab.waterdrop.spark.sink

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.commons.net.util.Base64
import org.apache.http.HttpHeaders
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, DefaultConnectionKeepAliveStrategy, DefaultRedirectStrategy, HttpClientBuilder}

import scala.util.{Failure, Success, Try}

object DorisUtil extends Serializable {

  val CHARSET = "UTF-8"
  val BINARY_CT = "application/octet-stream"
  val CONTENT_TYPE = "text/plain"
  var TIMEOUT = 30000

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
                 payload: String,
                 api: String,
                 headers: Map[String, String] = null,
                 user: String,
                 password: String): CloseableHttpResponse = {
    val httpPut = new HttpPut(api)
    val requestConfig = RequestConfig.custom()
      .setAuthenticationEnabled(true)
      .setCircularRedirectsAllowed(true)
      .setRedirectsEnabled(true)
      .setRelativeRedirectsAllowed(true)
      .setExpectContinueEnabled(true)
      .setConnectTimeout(TIMEOUT).setConnectionRequestTimeout(TIMEOUT)
      .setSocketTimeout(TIMEOUT).build()
    httpPut.setConfig(requestConfig)
    httpPut.setHeader(HttpHeaders.EXPECT, "100-continue")
    httpPut.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(user, password))
    if (headers != null && headers.nonEmpty) {
      headers.foreach(entry => {
        httpPut.setHeader(entry._1, entry._2)
      })
    }
    val content = new StringEntity(payload, Charset.forName(CHARSET))
    content.setContentType(CONTENT_TYPE)
    content.setContentEncoding(CHARSET)
    httpPut.setEntity(content)
    httpclient.execute(httpPut)
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
    val triedTuple = Try(DorisUtil.streamLoad(httpClient, messages, apiUrl, httpHeader, user, password))
    triedTuple match {
      case Success(_) =>
        httpClient.close()
        triedTuple.get.close()
      case Failure(var1: Exception) =>
        httpClient.close()
        triedTuple.get.close()
        throw var1
    }
  }
}
