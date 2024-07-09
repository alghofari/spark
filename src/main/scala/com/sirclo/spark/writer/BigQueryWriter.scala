package com.sirclo.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.sirclo.spark.helper.JsonHelper

case class BigQueryWriter() extends Writer {
  override def write(df: DataFrame, bqTable:String, optionConfig: Map[String, String]): Unit = {
    val writeLoadMethod = optionConfig.get("writeLoadMethod").flatMap(Option(_)).get
    val targetBQProject = optionConfig.get("targetBQProject").flatMap(Option(_)).get
    val partitionKey = optionConfig.getOrElse("partitionKey", "")

    // Define schema and parsing into json transformation
    val schemaJsonString = optionConfig.getOrElse("schemaJson", "")
    val formattedSchemaJsonString = schemaJsonString.replaceAll("\\s+", "")

    val schema: StructType = if (formattedSchemaJsonString.nonEmpty) {
      val newSchemaJsonString = JsonHelper.JsonTransformation(formattedSchemaJsonString)
      DataType.fromJson(newSchemaJsonString).asInstanceOf[StructType] // Transform data
    } else {
      df.schema
    }
    println(schema)

    // Apply the schema to the DataFrame
    val castedDataFrame = df.select(schema.fieldNames.map(name => col(name).cast(schema(name).dataType)): _*)

    writeLoadMethod match {
      case "append" =>
        castedDataFrame.write
          .format("com.google.cloud.spark.bigquery")
          .option("project_id", targetBQProject)
          .option("table", bqTable)
          .option("writeMethod", "direct")
          .option("partitioning", partitionKey)
          .option("schema", schemaJsonString)
          .mode(SaveMode.Append)
          .save()

      case "truncate" | "upsert" | "delsert" =>
        castedDataFrame.write
          .format("com.google.cloud.spark.bigquery")
          .option("project_id", targetBQProject)
          .option("table", bqTable)
          .option("writeMethod", "direct")
          .option("partitioning", partitionKey)
          .option("schema", schemaJsonString)
          .mode(SaveMode.Overwrite)
          .save()
    }

    println(s"Writing DataFrame from table $bqTable to BigQuery")
  }
}
