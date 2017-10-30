package org.interestinglab.waterdrop.utils

abstract class DateParser extends Serializable {

  def parse(input: String) : (Boolean, String)

  def parse(input: Long) : (Boolean, String)
}
