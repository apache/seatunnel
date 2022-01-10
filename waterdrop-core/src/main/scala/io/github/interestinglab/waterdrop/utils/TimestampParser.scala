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
import java.util.Date

import scala.util.control.NonFatal

class TimestampParser(targetTimeFormat: String) extends DateParser {

  val targetFormat = targetTimeFormat

  def parse(input: String): (Boolean, String) = {

    try {
      val inputDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS")
      parse(inputDateFormat.parse(input).getTime())
    } catch {
      case NonFatal(e) => (false, "")
    }
  }

  def parse(input: Long): (Boolean, String) = {

    val targetDateFormat = new SimpleDateFormat(this.targetFormat)
    val date = new Date(input)
    (true, targetDateFormat.format(date))
  }
}
