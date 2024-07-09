package com.sirclo.spark.reader

case class ReaderFactory() {
  def getReader(readerType: String,
                optionConfig: Map[String, String]): Reader = {
    readerType.toLowerCase match {
      case "jdbc" => JdbcReader(OptionConfig = optionConfig)
      case "bigquery" => BigqueryReader(OptionConfig = optionConfig)
      case _ => throw new IllegalArgumentException(s"Unsupported writer type: $readerType")
    }
  }
}