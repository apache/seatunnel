package io.github.interestinglab.waterdrop.utils

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.control.NonFatal

class UnixParser(targetTimeFormat: String) extends DateParser {

  val targetFormat = targetTimeFormat

  def parse(input: String): (Boolean, String) = {

    try {
      val timeMillis = input.toLong
      parse(timeMillis)
    } catch {
      case NonFatal(e) => (false, "")
    }
  }

  def parse(input: Long): (Boolean, String) = {

    val targetDateFormat = new SimpleDateFormat(this.targetFormat)
    val date = new Date(input * 1000)
    (true, targetDateFormat.format(date))
  }
}
