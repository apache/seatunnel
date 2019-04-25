package io.github.interestinglab.waterdrop.filter

import org.scalatest.FunSuite

class caseTest extends FunSuite {
  test("An empty Set should have size 0") {
    val a = 3

    a match {
      case 1
    }
    assert(Set.empty.size == 0)
  }
}
