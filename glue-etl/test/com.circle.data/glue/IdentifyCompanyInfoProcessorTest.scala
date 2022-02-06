package com.circle.data.glue

/*
 * Copyright (c) 2022 Circle Internet Financial Trading Company Limited.
 * All rights reserved.
 *
 * Circle Internet Financial Trading Company Limited CONFIDENTIAL
 *
 * This file includes unpublished proprietary source code of Circle Internet
 * Financial Trading Company Limited, Inc. The copyright notice above does not
 * evidence any actual or intended publication of such source code. Disclosure
 * of this source code or any related proprietary information is strictly
 * prohibited without the express written permission of Circle Internet Financial
 * Trading Company Limited.
 */
import com.circle.data.jobs.IdentifyCompanyInfoProcessor
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import utils.ETLSuiteBase

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class IdentifyCompanyInfoProcessorTest
  extends ETLSuiteBase {

  val companySchema = List(
    StructField("entity_ptr_id", IntegerType, nullable = true),
    StructField("legal_name", StringType, nullable = true),
    StructField("phone_number", StringType, nullable = true),
    StructField("company_entity_description", StringType, nullable = true)
  )

  val companyData = Seq(
    Row(474552, "Pongalo Holdings LLC", "15164549192", "New York Company"),
    Row(185255, "Panty Prop Incorporated", "15266666666", "A Delaware Corporation"),
    Row(155483, "Framework Fashion Inc.", "15164549192", "")
  )

  test("Test identify company info") {
    val companyDF = spark.createDataFrame(
      sparkContext.parallelize(companyData),
      StructType(companySchema)
    )

    val results = IdentifyCompanyInfoProcessor.getCompanyInfo(companyDF)

    assert(results.count() == 3)

    results.collect().foreach(System.out.println)

  }
}
