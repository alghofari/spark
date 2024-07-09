package com.sirclo.spark.helper

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case object JsonConfigParser {
  def parseJsonToCaseClass[T](jsonString: String, caseClassType: Class[T]): Either[String, T] = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    try {
      val config = objectMapper.readValue(jsonString, caseClassType)
      Right(config)
    } catch {
      case e: Exception =>
        Left("Error parsing JSON: " + e.getMessage)
    }
  }
}

