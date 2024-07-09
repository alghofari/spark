package com.sirclo.spark.helper

import scopt.OptionParser

object MainArgsParser {
  case class allConfig(
                        jobType: Option[String] = None,
                        config: Option[String] = None
                      )

  val parser: OptionParser[allConfig] =
    new OptionParser[allConfig]("sirclo-spark-pipeline") {
      opt[String]("job_type")
        .required()
        .action((value, config) => config.copy(jobType = Option(value)))

      opt[String]("job_config")
        .valueName("""example: {"jdbc_url":"jdbc:mysql://localhost:port/database_name", "jdbc_credential":"usernam:password"}""")
        .required()
        .action((value, config) => config.copy(config = Option(value)))
    }

  def parserArgs(args: Array[String]): Option[allConfig] = {
    parser.parse(args, allConfig())
  }
}
