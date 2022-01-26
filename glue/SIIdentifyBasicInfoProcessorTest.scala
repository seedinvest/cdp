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

package com.circle.data.glue

import com.circle.data.ETLSuiteBase
import com.circle.data.jobs.SIIdentifyBasicInfoProcessor
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class SIIdentifyBasicInfoProcessorTest
  extends ETLSuiteBase {

  val profileSchema = List(
    StructField("user_id", IntegerType, nullable = false),
    StructField("entity_ptr_id", IntegerType, nullable = true),
    StructField("date_joined", TimestampType, nullable = true),
    StructField("email_confirmed", BooleanType, nullable = true),
    StructField("user_request_to_be_forgotten_expiration_date", TimestampType, nullable = true),
    StructField("user_request_to_be_forgotten_notes", StringType, nullable = true),
    StructField("user_requested_to_be_forgotten_date", TimestampType, nullable = true)
  )
  val authSchema = List(
    StructField("id", IntegerType, nullable = false),
    StructField("email", StringType, nullable = true),
    StructField("date_joined", TimestampType, nullable = true),
    StructField("last_login", TimestampType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true)
  )
  val identitySchema = List(
    StructField("id", IntegerType, nullable = false),
    StructField("userprofile_id", IntegerType, nullable = true),
    StructField("modified_at", TimestampType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("phone_number", StringType, nullable = true),
    StructField("country_of_citizenship_id", IntegerType, nullable = true),
    StructField("date_of_birth", StringType, nullable = true)
  )
  val associationSchema = List(
    StructField("id", IntegerType, nullable = false),
    StructField("userprofile_id", IntegerType, nullable = true),
    StructField("title_at_current_employer", StringType, nullable = true),
    StructField("last_employer_name", StringType, nullable = true),
    StructField("current_employment_status", StringType, nullable = true),
    StructField("state", StringType, nullable = true)
  )
  val accreditationSchema = List(
    StructField("id", IntegerType, nullable = false),
    StructField("userprofile_id", IntegerType, nullable = true),
    StructField("is_accredited", BooleanType, nullable = true),
    StructField("is_accredited_via_entity", BooleanType, nullable = true),
    StructField("is_accredited_via_income", BooleanType, nullable = true),
    StructField("is_accredited_via_finra_license", BooleanType, nullable = true),
    StructField("is_accredited_via_trust", BooleanType, nullable = true),
    StructField("is_accredited_via_networth", BooleanType, nullable = true)
  )

  val timestamp1: Timestamp = Timestamp.valueOf("2020-01-01 23:00:01")
  val timestamp2: Timestamp = Timestamp.valueOf("2020-01-11 23:00:01")
  val timestamp3: Timestamp = Timestamp.valueOf("2020-01-21 23:00:01")
  val timestamp4: Timestamp = Timestamp.valueOf("2020-01-01 23:30:01")
  val timestamp5: Timestamp = Timestamp.valueOf("2020-01-11 23:30:01")
  val timestamp6: Timestamp = Timestamp.valueOf("2020-01-21 23:30:01")
  
  val profileData = Seq(
    Row(1, 1, timestamp1, true, timestamp1, "notes 1", timestamp4),
    Row(2, 2, timestamp2, false, timestamp2, "notes 2", timestamp5),
    Row(3, 3, timestamp3, true, timestamp3, "notes 3", timestamp6)
  )
  val authData = Seq(
    Row(1, "test1@email.com", timestamp1, timestamp4, "Bob", "Smith"),
    Row(2, "test2@email.com", timestamp2, timestamp5, "Alice", "Johnson"),
    Row(3, "test3@email.com", timestamp3, timestamp6, "Malory", "Williams")
  )
  val identityData = Seq(
    Row(1, 1, timestamp1, "Bob", "Smith", "1234567890", 1, "2010-01-01"),
    Row(2, 2, timestamp2, "Alice", "Johnson", "1234567891", 2, "2010-02-01"),
    Row(3, 3, timestamp3, "Malory", "Williams", "1234567892", 3, "2010-03-01")
  )
  val associationData = Seq(
    Row(1, 1, "Software Engineer", "Company 1", "Employed", "Alabama"),
    Row(2, 2, "Doctor", "Company 2", "Self-Employed", "Alaska"),
    Row(3, 3, "Investment Banker", "Company 3", "Non-Employed", "Arizona")
  )
  val accreditationData = Seq(
    Row(1, 1, true, true, true, true, true, true),
    Row(2, 2, false, false, false, false, false, false),
    Row(3, 3, true, false, true, false, true, false)
  )

  test("Test identify basic info") {
    val profileDF = spark.createDataFrame(
      sparkContext.parallelize(profileData),
      StructType(profileSchema)
    )
    val authDF = spark.createDataFrame(
      sparkContext.parallelize(authData),
      StructType(authSchema)
    )
    val identityDF = spark.createDataFrame(
      sparkContext.parallelize(identityData),
      StructType(identitySchema)
    )
    val associationDF = spark.createDataFrame(
      sparkContext.parallelize(associationData),
      StructType(associationSchema)
    )
    val accreditationDF = spark.createDataFrame(
      sparkContext.parallelize(accreditationData),
      StructType(accreditationSchema)
    )

    val results = SIIdentifyBasicInfoProcessor.getBasicInfo(profileDF, authDF, identityDF, associationDF, accreditationDF)

    assert(results.count() == 3)

    val row1 = results.filter(col("userId") === 1).first()
    assert(1 == row1.getAs[Int]("userId"))
    assert("test1@email.com".equals(row1.getAs[String]("traits.email")))
    assert(timestamp1.equals(row1.getAs[Timestamp]("traits.modified_at")))
    assert(timestamp1.equals(row1.getAs[Timestamp]("traits.date_joined")))
    assert(timestamp4.equals(row1.getAs[Timestamp]("traits.last_login")))
    assert("Bob".equals(row1.getAs[String]("traits.first_name")))
    assert("Smith".equals(row1.getAs[String]("traits.last_name")))
    assert("1234567890".equals(row1.getAs[String]("traits.phone_number")))
    assert(1 == row1.getAs[Int]("traits.country_of_citizenship_id"))
    assert("2010-01-01".equals(row1.getAs[String]("traits.date_of_birth")))
    assert(1 == row1.getAs[Int]("traits.user_identity_id"))
    assert(row1.getAs[Boolean]("traits.email_confirmed"))
    assert(1 == row1.getAs[Int]("traits.user_id"))
    assert(timestamp1.equals(row1.getAs[Timestamp]("traits.user_request_to_be_forgotten_expiration_date")))
    assert("notes 1".equals(row1.getAs[String]("traits.user_request_to_be_forgotten_notes")))
    assert(timestamp4.equals(row1.getAs[Timestamp]("traits.user_requested_to_be_forgotten_date")))
    assert(1 == row1.getAs[Int]("traits.user_accreditation_status_id"))
    assert(row1.getAs[Boolean]("traits.is_accredited"))
    assert(row1.getAs[Boolean]("traits.is_accredited_via_entity"))
    assert(row1.getAs[Boolean]("traits.is_accredited_via_income"))
    assert(row1.getAs[Boolean]("traits.is_accredited_via_finra_license"))
    assert(row1.getAs[Boolean]("traits.is_accredited_via_trust"))
    assert(row1.getAs[Boolean]("traits.is_accredited_via_networth"))
    assert("Software Engineer".equals(row1.getAs[String]("traits.title_at_current_employer")))
    assert("Company 1".equals(row1.getAs[String]("traits.last_employer_name")))
    assert("Employed".equals(row1.getAs[String]("traits.current_employer_status")))
    assert(1 == row1.getAs[Int]("traits.user_association_information_id"))
    assert("Alabama".equals(row1.getAs[String]("traits.state")))


    val row2 = results.filter(col("userId") === 2).first()
    assert(2 == row2.getAs[Int]("userId"))
    assert("test2@email.com".equals(row2.getAs[String]("traits.email")))
    assert(timestamp2.equals(row2.getAs[Timestamp]("traits.modified_at")))
    assert(timestamp2.equals(row2.getAs[Timestamp]("traits.date_joined")))
    assert(timestamp5.equals(row2.getAs[Timestamp]("traits.last_login")))
    assert("Alice".equals(row2.getAs[String]("traits.first_name")))
    assert("Johnson".equals(row2.getAs[String]("traits.last_name")))
    assert("1234567891".equals(row2.getAs[String]("traits.phone_number")))
    assert(2 == row2.getAs[Int]("traits.country_of_citizenship_id"))
    assert("2010-02-01".equals(row2.getAs[String]("traits.date_of_birth")))
    assert(2 == row2.getAs[Int]("traits.user_identity_id"))
    assert(!row2.getAs[Boolean]("traits.email_confirmed"))
    assert(2 == row2.getAs[Int]("traits.user_id"))
    assert(timestamp2.equals(row2.getAs[Timestamp]("traits.user_request_to_be_forgotten_expiration_date")))
    assert("notes 2".equals(row2.getAs[String]("traits.user_request_to_be_forgotten_notes")))
    assert(timestamp5.equals(row2.getAs[Timestamp]("traits.user_requested_to_be_forgotten_date")))
    assert(2 == row2.getAs[Int]("traits.user_accreditation_status_id"))
    assert(!row2.getAs[Boolean]("traits.is_accredited"))
    assert(!row2.getAs[Boolean]("traits.is_accredited_via_entity"))
    assert(!row2.getAs[Boolean]("traits.is_accredited_via_income"))
    assert(!row2.getAs[Boolean]("traits.is_accredited_via_finra_license"))
    assert(!row2.getAs[Boolean]("traits.is_accredited_via_trust"))
    assert(!row2.getAs[Boolean]("traits.is_accredited_via_networth"))
    assert("Doctor".equals(row2.getAs[String]("traits.title_at_current_employer")))
    assert("Company 2".equals(row2.getAs[String]("traits.last_employer_name")))
    assert("Self-Employed".equals(row2.getAs[String]("traits.current_employer_status")))
    assert(2 == row2.getAs[Int]("traits.user_association_information_id"))
    assert("Alaska".equals(row2.getAs[String]("traits.state")))

    val row3 = results.filter(col("userId") === 3).first()
    assert(3 == row3.getAs[Int]("userId"))
    assert("test3@email.com".equals(row3.getAs[String]("traits.email")))
    assert(timestamp3.equals(row3.getAs[Timestamp]("traits.modified_at")))
    assert(timestamp3.equals(row3.getAs[Timestamp]("traits.date_joined")))
    assert(timestamp6.equals(row3.getAs[Timestamp]("traits.last_login")))
    assert("Malory".equals(row3.getAs[String]("traits.first_name")))
    assert("Williams".equals(row3.getAs[String]("traits.last_name")))
    assert("1234567892".equals(row3.getAs[String]("traits.phone_number")))
    assert(3 == row3.getAs[Int]("traits.country_of_citizenship_id"))
    assert("2010-03-01".equals(row3.getAs[String]("traits.date_of_birth")))
    assert(3 == row3.getAs[Int]("traits.user_identity_id"))
    assert(row3.getAs[Boolean]("traits.email_confirmed"))
    assert(3 == row3.getAs[Int]("traits.user_id"))
    assert(timestamp3.equals(row3.getAs[Timestamp]("traits.user_request_to_be_forgotten_expiration_date")))
    assert("notes 3".equals(row3.getAs[String]("traits.user_request_to_be_forgotten_notes")))
    assert(timestamp6.equals(row3.getAs[Timestamp]("traits.user_requested_to_be_forgotten_date")))
    assert(3 == row3.getAs[Int]("traits.user_accreditation_status_id"))
    assert(row3.getAs[Boolean]("traits.is_accredited"))
    assert(!row3.getAs[Boolean]("traits.is_accredited_via_entity"))
    assert(row3.getAs[Boolean]("traits.is_accredited_via_income"))
    assert(!row3.getAs[Boolean]("traits.is_accredited_via_finra_license"))
    assert(row3.getAs[Boolean]("traits.is_accredited_via_trust"))
    assert(!row3.getAs[Boolean]("traits.is_accredited_via_networth"))
    assert("Investment Banker".equals(row3.getAs[String]("traits.title_at_current_employer")))
    assert("Company 3".equals(row3.getAs[String]("traits.last_employer_name")))
    assert("Non-Employed".equals(row3.getAs[String]("traits.current_employer_status")))
    assert(3 == row3.getAs[Int]("traits.user_association_information_id"))
    assert("Arizona".equals(row3.getAs[String]("traits.state")))
  }
}
