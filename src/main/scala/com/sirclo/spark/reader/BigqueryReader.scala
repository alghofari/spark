package com.sirclo.spark.reader

// Import packages
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BigqueryReader(OptionConfig: Map[String, String]) extends Reader {
  // Define URI variable, Username, and Password
  private val projectBQ = OptionConfig.get("BQProject").flatMap(Option(_)).get
  private val tempDataset = OptionConfig.get("tempDataset").flatMap(Option(_)).get
  private val queryReader = OptionConfig.get("queryReader").flatMap(Option(_)).get
  println(s"Start read data from project : $projectBQ")
  println(s"With query : $queryReader")

  // Perform the reader function
  override def readDataFrame(sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .format("com.google.cloud.spark.bigquery")
      .option("parentProject", projectBQ)
      .option("viewsEnabled", "true")
      .option("materializationDataset", tempDataset)
      .option("query", queryReader)
      .load()
  }
}