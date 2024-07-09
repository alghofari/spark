package com.sirclo.spark.job

import com.sirclo.spark.helper.JsonConfigParser
import com.sirclo.spark.reader.ReaderFactory
import com.sirclo.spark.writer.WriterFactory
import com.sirclo.spark.jobConfig.BigqueryToParquetConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object BigqueryToParquet extends Job {
  override def main(sparkSession: SparkSession, args: String): Unit = {
    val result = JsonConfigParser.parseJsonToCaseClass(args, classOf[BigqueryToParquetConfig])

    // Parse command-line arguments
    val optionConfig: Map[String, String] = result match {
      case Right(config) =>
        Map(
          "queryReader" -> config.queryReader,
          "BQProject" -> config.BQProject,
          "tempDataset" -> config.tempDataset,
          "hivePartitionKey" -> config.hivePartitionKey,
          "GCSFolder" -> config.GCSFolder,
          "GCSBucket" -> config.GCSBucket,
          "GCSProject" -> config.GCSProject,
        )

      case Left(error) =>
        Map("error" -> error)
    }

    // Access the properties as a Map
    println("Properties as Map:")
    optionConfig.foreach { case (key, value) =>
      println(s"$key: $value")
    }

    // Read Dataframe from Source
    println("Start Read DataFrame from Source")
    val readerType = "bigquery"
    val readerFactory = ReaderFactory()
    val reader = readerFactory.getReader(readerType, optionConfig)
    val df: DataFrame = reader.readDataFrame(sparkSession)
    df.show(numRows = 1)
    println("Finish Read DataFrame from Source")

    // Write dataframe into Destination
    def writeDataframe(dataFrame: DataFrame): Unit = {
      val GCSFolder = optionConfig.get("GCSFolder").flatMap(Option(_)).get
      val Array(datasetName, tableName) = GCSFolder.split("\\.")
      val GCSPath = s"$datasetName/$tableName"

      // Perform write from factory
      val writerFactory = WriterFactory()
      val writer = writerFactory.getWriter("parquet")
      writer.write(dataFrame, GCSPath, optionConfig)
    }

    println("Start Write DataFrame to Destination")
    writeDataframe(df)
    println("Finish Write DataFrame to Destination")

    // Stop the SparkSession
    println("Done extraction")
    sparkSession.stop()
  }
}
