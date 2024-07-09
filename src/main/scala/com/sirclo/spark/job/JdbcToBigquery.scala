package com.sirclo.spark.job

import com.sirclo.spark.helper.{BigQueryHelper, JsonConfigParser}
import com.sirclo.spark.reader.ReaderFactory
import com.sirclo.spark.writer.WriterFactory
import com.sirclo.spark.jobConfig.JdbcToBigqueryConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcToBigquery extends Job {
  override def main(sparkSession: SparkSession, args: String): Unit = {
    val result = JsonConfigParser.parseJsonToCaseClass(args, classOf[JdbcToBigqueryConfig])

    // Parse command-line arguments
    val optionConfig: Map[String, String] = result match {
      case Right(config) =>
        Map(
          "dbURL" -> config.dbURL,
          "jdbcCredential" -> config.jdbcCredential,
          "queryReader" -> config.queryReader,
          "writeLoadMethod" -> config.writeLoadMethod,
          "targetBQProject" -> config.targetBQProject,
          "partitionField" -> config.partitionField,
          "queryWriter" -> config.queryWriter,
          "pathMainTable" -> config.pathMainTable,
          "pathTempTable" -> config.pathTempTable,
          "schemaJson" -> config.schemaJson
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
    val readerType = "jdbc"
    val readerFactory = ReaderFactory()
    val reader = readerFactory.getReader(readerType, optionConfig) // Use the appropriate reader type
    val df: DataFrame = reader.readDataFrame(sparkSession)
    df.show(numRows = 1)
    println("Finish Read DataFrame from Source")

    // Write dataframe into Destination
    def writeDataframe(dataFrame: DataFrame): Unit = {

      val projectId = optionConfig.get("targetBQProject").flatMap(Option(_)).get
      val writeLoadMethod = optionConfig.get("writeLoadMethod").flatMap(Option(_)).get.toLowerCase
      val bqMainTable = optionConfig.get("pathMainTable").flatMap(Option(_)).get
      val schemaJson = optionConfig.get("schemaJson").flatMap(Option(_)).get
      val bqTempTable = optionConfig.get("pathTempTable").flatMap(Option(_)).get
      println(s"Start write into Project $projectId with method: $writeLoadMethod into table: $bqMainTable")

      // Perform write from factory
      val writerFactory = WriterFactory()
      val writer = writerFactory.getWriter("bigquery")

      // Check if Main Table Exists
      BigQueryHelper.checkTableExists(projectId, bqMainTable, schemaJson)

      // Perform write based on each method
      println(s"Start to perform $writeLoadMethod on table $bqMainTable")
      writeLoadMethod match {
        case "upsert" | "delsert" =>
          println(s"Start to perform $writeLoadMethod on table $bqMainTable")
          writer.write(dataFrame, bqTempTable, optionConfig)
          BigQueryHelper.queryToMainTable(optionConfig)
          BigQueryHelper.deleteTempTable(optionConfig)

        case "truncate" | "append" =>
          writer.write(dataFrame, bqMainTable, optionConfig)

        case "" =>
          throw new IllegalArgumentException(s"Unsupported write method: $writeLoadMethod")
      }
      println(s"Finish to perform $writeLoadMethod on table $bqMainTable")
    }

    println("Start Write DataFrame to Destination")
    writeDataframe(df)
    println("Finish Write DataFrame to Destination")

    // Stop the SparkSession
    println("Done extraction")
    sparkSession.stop()
  }
}
