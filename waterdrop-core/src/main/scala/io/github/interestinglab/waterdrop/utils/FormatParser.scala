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
package io.github.interestinglab.waterdrop.utils

import java.text.SimpleDateFormat
import java.util.Locale

import scala.util.control.NonFatal

class FormatParser(sourceTimeFormat: String, targetTimeFormat: String) extends DateParser {

  val sourceFormat = sourceTimeFormat
  val targetFormat = targetTimeFormat

  def this(targetFormat: String) {
    this("", targetFormat)
  }

  def parse(input: String): (Boolean, String) = {

    val locale = Locale.US
    val sourceDateFormat = new SimpleDateFormat(this.sourceFormat, locale)
    val targetDateFormat = new SimpleDateFormat(this.targetFormat, locale)

    try {
      val date = sourceDateFormat.parse(input)
      (true, targetDateFormat.format(date))
    } catch {
      case NonFatal(e) => (false, "")
    }
  }

  def parse(input: Long) : (Boolean, String) = {
    parse(input.toString)
  }
}
