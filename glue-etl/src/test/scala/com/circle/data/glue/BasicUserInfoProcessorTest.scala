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
class BasicUserInfoProcessorTest
  extends ETLSuiteBase {

  val profileSchema = List(
    StructField("entity_ptr_id", IntegerType, nullable = true),
    StructField("user_id", IntegerType, nullable = true),
    StructField("email_confirmed", BooleanType, nullable = true)
  )

  val authSchema = List(
    StructField("id", IntegerType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("last_login", DateType, nullable = true),
    StructField("date_joined", DateType, nullable = true)
  )

  val statusSchema = List(
    StructField("userprofile_id", IntegerType, nullable = true),
    StructField("is_accredited", BooleanType, nullable = true)
  )

  val profileData = Seq(
    Row(211921, 193663, true),
    Row(469225, 412193, false),
    Row(431253, 450024, true)
  )

  val authData = Seq(
    Row(193663, "Rodney", "Smith", "rodne@sor.com", null, null),
    Row(412193, "Charles", "Yeo", "char@atotech.com", null, null),
    Row(450024, "Nicholas", "Lu", "nick@ged.com", null, null)
  )
  
  val statusData = Seq(
    Row(211921, true),
    Row(469225, false),
    Row(431253, true)
  )

  test("Test basic user info") {

    val profileDF = spark.createDataFrame(
      sparkContext.parallelize(profileData),
      StructType(profileSchema)
    )

    val authDF = spark.createDataFrame(
      sparkContext.parallelize(authData),
      StructType(authSchema)
    )

    val statusDF = spark.createDataFrame(
      sparkContext.parallelize(statusData),
      StructType(statusSchema)
    )

    val results = getBasicUserData(authDF, profileDF, statusDF, false)

    assert(results.count() == 3)

    results.collect().foreach(System.out.println)

  }
}
