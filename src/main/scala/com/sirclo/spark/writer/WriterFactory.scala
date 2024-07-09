package com.sirclo.spark.writer

case class WriterFactory() {
  def getWriter(writerType: String): Writer = {
    writerType.toLowerCase match {
      case "bigquery" => BigQueryWriter()
      case "parquet" => ParquetWriter()
      case _ => throw new IllegalArgumentException(s"Unsupported writer type: $writerType")
    }
  }
}