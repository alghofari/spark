package com.sirclo.spark.writer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class ParquetWriterSpec
  extends AnyFlatSpec with Matchers with EitherValues
   {"ParquetWriter" should "throw an error when importing DateHeper.scala" in {
       val error = intercept[Throwable] {import com.sirclo.spark.helper.DateHelper}

       // Check that the error message contains the expected package name
       error.getMessage should include("com.project.spark.helper.DateHelper")
     }

   }
