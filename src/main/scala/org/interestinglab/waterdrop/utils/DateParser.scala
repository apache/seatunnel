package org.interestinglab.waterdrop.utils

abstract class DateParser {

  def parse(input: String) : (Boolean, String)
}
