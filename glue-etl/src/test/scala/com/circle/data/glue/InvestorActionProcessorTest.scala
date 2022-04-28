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
class InvestorActionProcessorTest
  extends ETLSuiteBase {

  val profileSchema = List(
    StructField("si_userprofile_id", IntegerType, nullable = true),
    StructField("id", IntegerType, nullable = true),
    StructField("email_confirmed", BooleanType, nullable = true),
    StructField("date_joined", DateType, nullable = true)
  )

  val activitySchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("event_action_id", IntegerType, nullable = true),
    StructField("actor_id", IntegerType, nullable = true),
    StructField("created_at", StringType, nullable = true),
    StructField("event_object", StringType, nullable = true),
    StructField("event_details", StringType, nullable = true)
  )

  val actionSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true)
  )

  val profileData = Seq(
    Row(211921, 193663, true, null),
    Row(469225, 412193, false, null),
    Row(431253, 450024, true, null),
    Row(431254, 450025, true, null)
  )

  val activityData = Seq(
    Row(1, 19, 193663, "2022-04-08 20:33:04.338868+00", null, null),
    Row(2, 31, 412193, null, null, null),
    Row(3, 22, 450024, null, null, null),
    Row(3, 22, 45002, null, null, null)
  )
  
  val actionData = Seq(
    Row(19, "AUTO_INVEST_MEMBERSHIP_ACTIVE"),
    Row(31, "AUTO_INVEST_MEMBERSHIP_PENDING_APPROVAL"),
    Row(22, "AUTO_INVEST_MEMBERSHIP_PAUSED"),
    Row(1, "USER_CANCELED_INVESTMENT")
  )

  test("Test investor action info") {

    val activityDF = spark.createDataFrame(
      sparkContext.parallelize(activityData),
      StructType(activitySchema)
    )

    val profileDF = spark.createDataFrame(
      sparkContext.parallelize(profileData),
      StructType(profileSchema)
    )

    val actionDF = spark.createDataFrame(
      sparkContext.parallelize(actionData),
      StructType(actionSchema)
    )

    val results = getInvestorActionData(activityDF, profileDF, actionDF)

    assert(results.count() == 3)

    results.collect().foreach(System.out.println)

  }
}
