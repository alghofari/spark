package com.sirclo.spark

import com.sirclo.spark.job.JdbcToBigquery
import com.sirclo.spark.job.BigqueryToParquet
import com.sirclo.spark.helper.{MainArgsParser, SparkSessionHelper}

object Main extends App with SparkSessionHelper{
  MainArgsParser.parserArgs(args) match {
    case Some(arguments) => main(arguments)
  }

  def main(args: MainArgsParser.allConfig): Unit = {
    val jobType = args.jobType.get
    jobType match {
      case "jdbc-to-bigquery" =>
        JdbcToBigquery.main(sparkSession = sparkSession,
                            args = args.config.get)

      case "bigquery-to-parquet" =>
        BigqueryToParquet.main(sparkSession = sparkSession,
                               args = args.config.get)
    }
  }
}

