package com.sirclo.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.sirclo.spark.helper.DateHelper

case class ParquetWriter() extends Writer {

  override def write(df: DataFrame, pathWrite: String, optionConfig: Map[String, String]): Unit = {
    val projectId = optionConfig.get("GCSProject").flatMap(Option(_)).get
    val GCSBucket = optionConfig.get("GCSBucket").flatMap(Option(_)).get
    val partitionKey = optionConfig.get("hivePartitionKey").flatMap(Option(_)).get

    val gcsDestinationPath: String = if (partitionKey != "") {
      "gs://" + GCSBucket + "/" + pathWrite + "/" + partitionKey + "=" + DateHelper.getDateString
    } else {
      "gs://" + GCSBucket + "/" + pathWrite + "/" + DateHelper.getDateString
    }

    println(s"Write dataframe into $gcsDestinationPath")

    df.write
      .format(source = "parquet")
      .option("project_id", projectId)
      .option("path", gcsDestinationPath)
      .mode(SaveMode.Overwrite)
      .save()

  }
}
