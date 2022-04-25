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
import com.circle.data.utils.QueryBase._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import utils.ETLSuiteBase

@RunWith(classOf[JUnitRunner])
class EmailPreferenceProcessorTest
  extends ETLSuiteBase {

  val ruleSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("created_at", DateType, nullable = true),
    StructField("modified_at", DateType, nullable = true),
    StructField("active", BooleanType, nullable = true),
    StructField("contact_channel_id", IntegerType, nullable = true),
    StructField("preference_id", IntegerType, nullable = true)
  )

  val channelSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("contact_method", StringType, nullable = true),
    StructField("contact_info", StringType, nullable = true)
  )

  val preferenceSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("key", StringType, nullable = true),
    StructField("label", StringType, nullable = true),
    StructField("display_label", StringType, nullable = true)
  )

  val ruleData = Seq(
    Row(1, null, null, true, 11, 21),
    Row(2, null, null, false, 12, 22),
    Row(3, null, null, true, 13, 22),
    Row(4, null, null, false, 14, 22)
  )

  val channelData = Seq(
    Row(11, "marketing", "foo@bar.com"),
    Row(12, "marketing", "foo1@bar.bar.com"),
    Row(13, "marketing", "foobar@"),
    Row(14, "marketing", "foobar")
  )
  
  val preferenceData = Seq(
    Row(21, "weekly_news_letter", "weekly_news_letter", "Weekly News Letter"),
    Row(22, "monthly_news_letter", "monthly_news_letter", "Monthly News Letter")
  )

  test("Test email preference") {

    val ruleDF = spark.createDataFrame(
      sparkContext.parallelize(ruleData),
      StructType(ruleSchema)
    )

    val channelDF = spark.createDataFrame(
      sparkContext.parallelize(channelData),
      StructType(channelSchema)
    )

    val preferenceDF = spark.createDataFrame(
      sparkContext.parallelize(preferenceData),
      StructType(preferenceSchema)
    )

    val results = getEmailPreferenceData(ruleDF, channelDF, preferenceDF)

    assert(results.count() == 2)

    results.collect().foreach(System.out.println)

  }
}
