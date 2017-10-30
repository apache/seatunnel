package org.interestinglab.waterdrop.utils

import java.text.SimpleDateFormat
import java.util.Date

class UnixMSParser(targetTimeFormat: String) extends DateParser {

  val targetFormat = targetTimeFormat

  def parse(input: String): (Boolean, String) = {

    try {
      val timeMillis = input.toLong
      parse(timeMillis)
    } catch {
      case _: Exception => (false, "")
    }
  }

  def parse(input: Long): (Boolean, String) = {

    val targetDateFormat = new SimpleDateFormat(this.targetFormat)
    val date = new Date(input)
    (true, targetDateFormat.format(date))
  }
}
