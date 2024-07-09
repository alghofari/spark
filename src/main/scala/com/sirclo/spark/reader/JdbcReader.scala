package com.sirclo.spark.reader

// Import packages
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.net.URI

case class JdbcReader(OptionConfig: Map[String, String]) extends Reader {
  // Define URI variable, Username, and Password
  private val dbURL = OptionConfig.get("dbURL").flatMap(Option(_)).get
  private val queryReader = OptionConfig.get("queryReader").flatMap(Option(_)).get
  private val jdbcCredential = OptionConfig.get("jdbcCredential").flatMap(Option(_)).get

  private val jdbcUsername = jdbcCredential.split(":")(0)
  private val jdbcPassword = jdbcCredential.split(":")(1)

  // Define the Driver
  private val jdbcDriver: String =
    new URI(new URI(dbURL).getSchemeSpecificPart).getScheme match {
      case "postgresql" => "org.postgresql.Driver"
      case "mysql" => "com.mysql.cj.jdbc.Driver"
      case _ => throw new IllegalArgumentException("Unsupported database type")
    }

  // Perform the reader function
  override def readDataFrame(sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .format("jdbc")
      .option("url", dbURL)
      .option("driver", jdbcDriver)
      .option("dbtable", queryReader)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .load()
  }
}
