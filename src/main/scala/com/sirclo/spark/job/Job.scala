package com.sirclo.spark.job

import org.apache.spark.sql.SparkSession

trait Job {
  def main(sparkSession: SparkSession, args: String): Unit
}