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

  val authSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("last_login", DateType, nullable = true),
    StructField("date_joined", DateType, nullable = true)
  )

  val profileSchema = List(
    StructField("entity_ptr_id", IntegerType, nullable = true),
    StructField("user_id", IntegerType, nullable = true),
    StructField("email_confirmed", BooleanType, nullable = true)
  )

  val ruleSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("created_at", StringType, nullable = true),
    StructField("modified_at", StringType, nullable = true),
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

  val profileData = Seq(
    Row(211921, 193663, true),
    Row(469225, 412193, false),
    Row(431253, 450024, true),
    Row(431254, 450025, true),
    Row(431255, 450026, true)
  )

  val authData = Seq(
    Row(193663, "Rodney", "Smith", "rodne@sor.com", null, null),
    Row(412193, "Charles", "Yeo", "char@atotech.com.uk", null, null),
    Row(450024, "Nicholas", "Lu", "nick@ged.com", null, null),
    Row(450025, "Nicholas", "Lu", "foobar@", null, null),
    Row(450026, "Jay", "Han", "foobar", null, null)
  )

  val ruleData = Seq(
    Row(1, null, "2022-04-08T20:33:04.338Z", true, 11, 21),
    Row(2, null, "2022-04-18T20:33:04.338Z", true, 12, 22),
    Row(3, null, null, true, 13, 22),
    Row(4, null, null, false, 14, 22),
    Row(5, null, "2022-04-18T20:33:04.338Z", true, 11, 22),
    Row(5, null, "2022-04-18T20:33:04.338Z", false, 11, 23)
  )

  val channelData = Seq(
    Row(11, "marketing", "rodne@sor.com"),
    Row(12, "marketing", "char@atotech.com.uk"),
    Row(13, "marketing", "foobar@"),
    Row(14, "marketing", "foobar")
  )
  
  val preferenceData = Seq(
    Row(21, "weekly_news_letter", "weekly_news_letter", "Weekly News Letter"),
    Row(22, "monthly_news_letter", "monthly_news_letter", "Monthly News Letter"),
    Row(23, "corp1", "corp1", "Corp1")
  )

  test("Test email preference") {

    val authDF = spark.createDataFrame(
      sparkContext.parallelize(authData),
      StructType(authSchema)
    )

    val profileDF = spark.createDataFrame(
      sparkContext.parallelize(profileData),
      StructType(profileSchema)
    )

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

    val results = getEmailPreferenceData(authDF, profileDF, ruleDF, channelDF, preferenceDF)

    assert(results.count() == 2)

    results.collect().foreach(System.out.println)

  }
}
