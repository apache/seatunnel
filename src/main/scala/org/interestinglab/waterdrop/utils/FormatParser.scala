package org.interestinglab.waterdrop.utils

import java.text.SimpleDateFormat

class FormatParser(sourceTimeFormat: String, targetTimeFormat: String) extends DateParser{

  val sourceFormat = sourceTimeFormat
  val targetFormat = targetTimeFormat

  def this(targetFormat: String) {
    this("", targetFormat)
  }

  def parse(input: String): (Boolean, String) = {

    val sourceDateFormat = new SimpleDateFormat(this.sourceFormat)
    val targetDateFormat = new SimpleDateFormat(this.targetFormat)

    try {
      val date = sourceDateFormat.parse(input)
      (true, targetDateFormat.format(date))
    } catch {
      case _: Throwable => {
        (false, "")
      }
    }
  }
}
