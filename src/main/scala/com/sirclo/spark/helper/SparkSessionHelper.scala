package com.sirclo.spark.helper

import org.apache.spark.sql.SparkSession

trait SparkSessionHelper extends Serializable {
  val sparkSession = SparkSession
    .builder()
    .getOrCreate()
}