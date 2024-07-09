package com.sirclo.spark.helper

object ParseArgsHelper {
  def parseArgs(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      if (arg.startsWith("--") && arg.drop(2).contains("=")) {
        val keyValue = arg.drop(2).split("=", 2)
        if (keyValue.length == 2) {
          val key = keyValue(0)
          println(keyValue(1))
          val value = keyValue(1) // Rejoin multiple equals signs in the value
          Some(key -> removeQuotes(value))
        } else {
          None
        }
      } else {
        None
      }
    }.toMap
  }

  private def removeQuotes(s: String): String = {
    if (s.startsWith("\"") && s.endsWith("\"")) {
      s.stripPrefix("\"").stripSuffix("\"")
    } else {
      s
    }
  }
}
