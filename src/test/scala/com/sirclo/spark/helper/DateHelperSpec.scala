package com.sirclo.spark.helper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateHelperSpec extends AnyFlatSpec with Matchers {
  "DateHelper" should "not compile" in {
    // Attempt to import the incorrect package
    // This will trigger a compilation error
    assertDoesNotCompile("import com.sirclo.spark.helper.DateHeper._")
  }
}