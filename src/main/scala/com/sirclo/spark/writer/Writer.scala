package com.sirclo.spark.writer

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame, bqTable:String, optionConfig: Map[String, String]): Unit
}