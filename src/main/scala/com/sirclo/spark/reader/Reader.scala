package com.sirclo.spark.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  def readDataFrame(sparkSession: SparkSession): DataFrame
}
