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
