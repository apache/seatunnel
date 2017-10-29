package org.interestinglab.waterdrop.utils

import java.util.Date

abstract class DateParser {

  def parse(input: String) : (Boolean, String)
}
