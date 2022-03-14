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

import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._

import com.norbitltd.spoiwo.model.Workbook
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.typesafe.config.ConfigFactory
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.seatunnel.common.config.{CheckConfigUtil, CheckResult}
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}
import play.api.libs.mailer.{Attachment, AttachmentData, Email, SMTPConfiguration, SMTPMailer}

class Email extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {

    // Get xlsx file's byte array
    val headerRow = Some(data.schema.fields.map(_.name).toSeq)
    val limitCount = if (config.hasPath("limit")) config.getInt("limit") else 100000
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

    val subject = if (config.hasPath("subject")) config.getString("subject") else "(No subject)"
    // Who sent the mail
    val from = config.getString("from")
    // Who receive mail
    val to = config.getString("to").split(",")

    val bodyText = if (config.hasPath("bodyText")) Some(config.getString("bodyText")) else None
    // Hypertext content, If bodyHtml is set, then bodeText will not take effect.
    val bodyHtml = if (config.hasPath("bodyHtml")) Some(config.getString("bodyHtml")) else None

    val cc =
      if (config.hasPath("cc")) config.getString("cc").split(",").map(_.trim()).filter(_.nonEmpty)
      else Array[String]()
    val bcc =
      if (config.hasPath("bcc")) {
        config.getString("bcc").split(",").map(_.trim()).filter(_.nonEmpty)
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
    val host = config.getString("host")
    val port = config.getInt("port")
    val password = config.getString("password")

    val mailer: SMTPMailer = createMailer(host, port, from, password)
    val result: String = mailer.send(email)
  }

  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "from", "to", "host", "port", "password")
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {}

  def createMailer(
      host: String,
      port: Int,
      user: String,
      password: String,
      timeout: Int = 10000,
      connectionTimeout: Int = 10000): SMTPMailer = {
    // STMP's service SMTPConfiguration
    val configuration = new SMTPConfiguration(
      host,
      port,
      false,
      false,
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
}
