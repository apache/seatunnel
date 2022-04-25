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
package org.apache.seatunnel.spark.email.sink

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._
import com.norbitltd.spoiwo.model.Workbook
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.typesafe.config.ConfigFactory
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.seatunnel.spark.email.Config.{BCC, BODY_HTML, BODY_TEXT, CC, DEFAULT_LIMIT, FROM, HOST, LIMIT, PASSWORD, PORT, SUBJECT, TO, USE_SSL, USE_TLS}
import org.apache.spark.sql.{Dataset, Row}
import play.api.libs.mailer.{Attachment, AttachmentData, Email, SMTPConfiguration, SMTPMailer}

class Email extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {

    // Get xlsx file's byte array
    val headerRow = Some(data.schema.fields.map(_.name).toSeq)
    val limitCount = if (config.hasPath(LIMIT)) config.getInt(LIMIT) else DEFAULT_LIMIT
    val dataRows = data.limit(limitCount)
      .toLocalIterator()
      .asScala
      .map(_.toSeq)
    val dataLocator = DataLocator(Map())
    val xssFWorkbook = new XSSFWorkbook()
    val sheet = dataLocator.toSheet(headerRow, dataRows, xssFWorkbook)
    val out = new ByteArrayOutputStream()
    Workbook(sheet).writeToOutputStream(out)
    out.flush()
    val xlsxBytes = out.toByteArray

    // Mail attachment
    val attachment: Attachment = AttachmentData(
      "result.xlsx",
      xlsxBytes,
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    val subject = if (config.hasPath(SUBJECT)) config.getString(SUBJECT) else "(No subject)"
    // Who sent the mail
    val from = config.getString(FROM)
    // Who receive mail
    val to = config.getString(TO).split(",")

    val bodyText = if (config.hasPath(BODY_TEXT)) Some(config.getString(BODY_TEXT)) else None
    // Hypertext content, If bodyHtml is set, then bodeText will not take effect.
    val bodyHtml = if (config.hasPath(BODY_HTML)) Some(config.getString(BODY_HTML)) else None

    val cc =
      if (config.hasPath(CC)) config.getString(CC).split(",").map(_.trim()).filter(_.nonEmpty)
      else Array[String]()
    val bcc =
      if (config.hasPath(BCC)) {
        config.getString(BCC).split(",").map(_.trim()).filter(_.nonEmpty)
      } else {
        Array[String]()
      }
    val email = Email(
      subject,
      from,
      to,
      bodyText = bodyText,
      bodyHtml = bodyHtml,
      charset = Option[String]("utf-8"),
      attachments = Array(attachment),
      cc = cc,
      bcc = bcc)

    // Mailbox server settings, used to send mail
    val host = config.getString(HOST)
    val port = config.getInt(PORT)
    val password = config.getString(PASSWORD)
    val ssl = if (config.hasPath(USE_SSL)) config.getBoolean(USE_SSL) else false
    val tls = if (config.hasPath(USE_TLS)) config.getBoolean(USE_TLS) else false

    val mailer: SMTPMailer = createMailer(host, port, from, password, ssl, tls)
    val result: String = mailer.send(email)
  }

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, FROM, TO, HOST, PORT, PASSWORD)
  }

  def createMailer(
      host: String,
      port: Int,
      user: String,
      password: String,
      ssl: Boolean = false,
      tls: Boolean = false,
      timeout: Int = 10000,
      connectionTimeout: Int = 10000
      ): SMTPMailer = {
    // STMP's service SMTPConfiguration
    val configuration = new SMTPConfiguration(
      host,
      port,
      ssl,
      tls,
      false,
      Option(user),
      Option(password),
      false,
      timeout = Option(timeout),
      connectionTimeout = Option(connectionTimeout),
      ConfigFactory.empty(),
      false)
    val mailer: SMTPMailer = new SMTPMailer(configuration)
    mailer
  }

  /**
   * Return the plugin name, this is used in seatunnel conf DSL.
   *
   * @return plugin name.
   */
  override def getPluginName: String = "Email"
}
