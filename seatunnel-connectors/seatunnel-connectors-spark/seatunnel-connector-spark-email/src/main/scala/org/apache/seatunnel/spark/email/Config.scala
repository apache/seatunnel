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
package org.apache.seatunnel.spark.email

/**
 * Configurations for Email connector
 */
object Config extends Serializable {
  /**
   * Email server host
   */
  val HOST = "host"

  /**
   * Email host port
   */
  val PORT = "port"

  /**
   * User or sender password
   */
  val PASSWORD = "password"

  /**
   * Email sender
   */
  val FROM = "from"

  /**
   * Email recepients
   */
  val TO = "to"

  /**
   * Default rows to limit
   */
  val DEFAULT_LIMIT = 100000

  /**
   * Number of rows to include
   */
  val LIMIT = "limit"

  /**
   * Email subject
   */
  val SUBJECT = "subject"

  /**
   * Email content text format
   */
  val BODY_TEXT = "bodyText"

  /**
   * Email content html format
   */
  val BODY_HTML = "bodyHtml"

  /**
   * Email BCC
   */
  val BCC = "bcc"

  /**
   * Email CC
   */
  val CC = "cc"

  /**
   * Whether to use ssl
   */
  val USE_SSL = "use_ssl"

  /**
   * Whether to use tls
   */
  val USE_TLS = "use_tls"


}
