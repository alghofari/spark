package com.sirclo.spark.helper

import play.api.libs.json._

object JsonHelper {
  def JsonTransformation(inputJson: String): String = {
    val doubleQuotedInputList = inputJson.replaceAll("'", "\"")
    val inputList = Json.parse(doubleQuotedInputList).as[List[JsObject]]

    val outputFields = inputList.map { obj =>
      val name = (obj \ "name").as[String]
      val fieldType = (obj \ "type").as[String].toLowerCase
      val nullable = (obj \ "mode").as[String] == "NULLABLE"

      Json.obj(
        "name" -> name,
        "type" -> fieldType,
        "nullable" -> nullable,
        "metadata" -> Json.obj("scale" -> 0)
      )
    }

    val outputJson = Json.obj(
      "type" -> "struct",
      "fields" -> outputFields
    )

    Json.prettyPrint(outputJson)

  }
}

