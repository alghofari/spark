package com.sirclo.spark.helper

import com.google.cloud.bigquery.{BigQuery,
  BigQueryOptions,
  QueryJobConfiguration,
  StandardSQLTypeName,
  StandardTableDefinition,
  Field,
  TableId,
  TableInfo}

import com.google.cloud.bigquery.Schema

import io.circe.parser._
import io.circe.{Decoder, HCursor}

object BigQueryHelper {

  // Function for delete table
  def deleteTempTable(optionConfig: Map[String, String]): Unit = {
    val bqMainTable = optionConfig.get("pathMainTable").flatMap(Option(_)).get
    val targetBQProject = optionConfig.get("targetBQProject").flatMap(Option(_)).get
    val bqTempTable = bqMainTable + "_temp"
    val bqTable = getTableId(bqTempTable)

    // Delete the temporary table
    val bigquery = BigQueryOptions.newBuilder().setProjectId(targetBQProject).build().getService
    bigquery.getTable(bqTable).delete()
    println(s"Deleting $bqTempTable table")
  }

  // Function for run query to main table
  def queryToMainTable(optionConfig: Map[String, String]): Unit = {
    val performQuery = optionConfig("queryWriter")
    val targetBQProject = optionConfig.get("targetBQProject").flatMap(Option(_)).get
    println(performQuery)

    // Perform query to the main table
    val bigquery = BigQueryOptions.newBuilder().setProjectId(targetBQProject).build().getService

    val queryConfig = QueryJobConfiguration.newBuilder(performQuery).build()
    bigquery.query(queryConfig)
    println("Perform query to the main table")
  }

  // Function for get table id to define each necessary identifier
  private def getTableId(tableReference: String): TableId = {
    val parts = tableReference.split("\\.")
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid table reference format")
    }
    TableId.of(parts(0), parts(1), parts(2))
  }

  // Function for check table if exist
  def checkTableExists(projectId: String, tableReference: String, schemaJson: String): Unit = {
    val bqTable = getTableId(tableReference)

    val bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService
    val table = bigquery.getTable(bqTable)

    if (table == null) {
      // The table doesn't exist, so create it
      createTable(tableReference, schemaJson)
    }
    else {
      println(s"The main table fo $tableReference is existed")
    }
  }

  // Create Empty Table into BigQuery
  private def createTable(fullTargetBqTable: String, schemaJson: String): Unit = {
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

    // Parse the schema JSON string into a list of BigQuery Fields
    val schemaFields = parseSchema(schemaJson)

    // Split the full_target_bq_table to get project, dataset, and table name
    val tableComponents = fullTargetBqTable.split("\\.")
    if (tableComponents.length != 3) {
      throw new IllegalArgumentException("Invalid full_target_bq_table format. Expected format: project.dataset.table")
    }

    val projectId = tableComponents(0)
    val datasetId = tableComponents(1)
    val tableId = tableComponents(2)

    // Define the schema for the table
    val tableSchema = Schema.of(schemaFields: _*) // Convert schemaFields to Schema

    // Create a TableId with project, dataset, and table name
    val tableIdObject = TableId.of(projectId, datasetId, tableId)

    // Define the table definition
    val tableDefinition = StandardTableDefinition.newBuilder()
      .setSchema(tableSchema)
      .build()

    // Create the table in BigQuery
    val tableInfo = TableInfo.newBuilder(tableIdObject, tableDefinition).build()
    val createdTable = bigquery.create(tableInfo)

    println(s"Table ${createdTable.getTableId} created.")
  }

  // Parse the JSON schema string into a Schema object
  private def parseSchema(schemaJson: String): Seq[Field] = {
    // Define a case class for the schema
    case class SchemaField(name: String, `type`: String, mode: Option[String])

    // Define a custom Circe decoder for SchemaField
    implicit val schemaFieldDecoder: Decoder[SchemaField] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        dataType <- c.downField("type").as[String]
        mode <- c.downField("mode").as[Option[String]]
      } yield SchemaField(name, dataType, mode)

    // Parse the JSON string into a list of SchemaField objects
    val parsedSchema = decode[Seq[SchemaField]](schemaJson) match {
      case Right(fields) => fields
      case Left(error) =>
        throw new IllegalArgumentException(s"Failed to parse schema JSON: $error")
    }

    // Convert SchemaField objects to BigQuery Fields
    parsedSchema.map { field =>
      val fieldType = StandardSQLTypeName.valueOf(field.`type`)
      val fieldMode = field.mode.getOrElse("NULLABLE")
      Field.newBuilder(field.name, fieldType).setMode(Field.Mode.valueOf(fieldMode)).build()
    }
  }

}
