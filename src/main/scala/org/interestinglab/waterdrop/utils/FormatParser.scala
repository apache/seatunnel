package org.interestinglab.waterdrop.utils

import java.text.SimpleDateFormat
import java.util.Date

class FormatParser(sourceTimeFormat: String, targetTimeFormat: String) extends DateParser{

  val sourceFormat = sourceTimeFormat
  val targetFormat = targetTimeFormat

  def this(targetFormat: String) {
    this("", targetFormat)
  }

  override def parse(input: String): (Boolean, String) = {

    val dateFormat = new SimpleDateFormat(this.sourceFormat)
    try {
      val date = dateFormat.parse(input)
      (true, "")
    } catch {
      case _: Throwable => {
        (false, "")
      }
    }
  }
}
