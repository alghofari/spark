package com.sirclo.spark.helper

import java.time.LocalDate

object DateHelper {
  def getDateString: String = {
    val currentDate: LocalDate = LocalDate.now()
    currentDate.toString
  }
}