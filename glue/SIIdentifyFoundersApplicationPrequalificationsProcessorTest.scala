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
import com.circle.data.jobs.SIIdentifyFoundersApplicationPrequalificationsProcessor
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class SIIdentifyFoundersApplicationPrequalificationsProcessorTest
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
    val appSchema = List(
      StructField("id", IntegerType, nullable = false),
      StructField("pre_qualification_id", IntegerType, nullable = true),
      StructField("main_poc_id", IntegerType, nullable = true),
      StructField("deal_id", IntegerType, nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("submitted_at", TimestampType, nullable = true),
      StructField("approved_at", TimestampType, nullable = true),
      StructField("rejected_at", TimestampType, nullable = true),
      StructField("modified_at", TimestampType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("user_base_count", LongType, nullable = true),
      StructField("latest_pitch_deck", StringType, nullable = true),
      StructField("market_landscape_notes", StringType, nullable = true),
      StructField("preliminary_approval_notes", StringType, nullable = true),
      StructField("key_members_json", StringType, nullable = true),
      StructField("supplements_json", StringType, nullable = true),
      StructField("current_round_json", StringType, nullable = true)
    )
    val preqSchema = List(
      StructField("id", IntegerType, nullable = false),
      StructField("first_name", StringType, nullable = true),
      StructField("last_name", StringType, nullable = true),
      StructField("phone_number", StringType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("venture_member_email", StringType, nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("submitted_at", TimestampType, nullable = true),
      StructField("approved_at", TimestampType, nullable = true),
      StructField("rejected_at", TimestampType, nullable = true),
      StructField("source_name", StringType, nullable = true),
      StructField("source_medium", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("country_of_company_id", IntegerType, nullable = true),
      StructField("company_legal_name", StringType, nullable = true),
      StructField("company_name", StringType, nullable = true),
      StructField("company_url", StringType, nullable = true),
      StructField("title_at_company", StringType, nullable = true),
      StructField("full_time_employees", IntegerType, nullable = true),
      StructField("raise_amount", DoubleType, nullable = true),
      StructField("raise_amount_currency", StringType, nullable = true),
      StructField("latest_pitch_deck", StringType, nullable = true),
      StructField("has_started_current_fundraise", BooleanType, nullable = true),
      StructField("is_first_raise", BooleanType, nullable = true),
      StructField("is_faked", BooleanType, nullable = true),
      StructField("is_testing", BooleanType, nullable = true)
    )

    val timestamp1: Timestamp = Timestamp.valueOf("2020-01-01 23:00:01")
    val timestamp2: Timestamp = Timestamp.valueOf("2020-01-11 23:00:01")
    val timestamp3: Timestamp = Timestamp.valueOf("2020-01-21 23:00:01")

    val profileData = Seq(
      Row(1, 1, timestamp1, true, timestamp1, "notes 1", timestamp1),
      Row(2, 2, timestamp2, false, timestamp2, "notes 2", timestamp2),
      Row(3, 3, timestamp3, true, timestamp3, "notes 3", timestamp3)
    )
    val authData = Seq(
      Row(1, "test1@email.com", timestamp1, timestamp1, "Bob", "Smith"),
      Row(2, "test2@email.com", timestamp2, timestamp2, "Alice", "Johnson"),
      Row(3, "test3@email.com", timestamp3, timestamp3, "Malory", "Williams")
    )
    val appData = Seq(
      Row(1, 1, 1, 1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, "APPROVED", 10000000000L, "pitch1",
        "note1", "note1", "json1", "json1", "json1"),
      Row(2, 2, 2, 2, timestamp2, timestamp2, timestamp2, timestamp2, timestamp2, "REJECTED", 20000000000L, "pitch2",
        "note2", "note2", "json2", "json2", "json2"),
      Row(3, 3, 3, 3, timestamp3, timestamp3, timestamp3, timestamp3, timestamp3, "CANCELED", 30000000000L, "pitch3",
        "note3", "note3", "json3", "json3", "json3")
    )
    val preqData = Seq(
      Row(1, "Bob", "Smith", "+12345678901", "test1@email.com", "test1@email.com", timestamp1, timestamp1,
        timestamp1, timestamp1, "source1", "source1", "APPROVED", 1, "comp1", "comp1", "comp1", "title1", 100, 100.00,
        "USD", "pitch1", true, true, true, true),
      Row(2, "Alice", "Johnson", "+12345678902", "test2@email.com", "test2@email.com", timestamp1, timestamp1,
        timestamp1, timestamp1, "source2", "source2", "REJECTED", 2, "comp2", "comp2", "comp2", "title2", 200, 200.00,
        "USD", "pitch2", true, true, true, true),
      Row(3, "Malory", "Williams", "+12345678903", "test3@email.com", "test3@email.com", timestamp1, timestamp1,
        timestamp1, timestamp1, "source3", "source3", "CANCELLED", 3, "comp3", "comp3", "comp3", "title3", 300, 300.00,
        "USD", "pitch3", true, true, true, true)
    )

    test("Test identify founders with application and prequalifications") {
      val profileDF = spark.createDataFrame(
        sparkContext.parallelize(profileData),
        StructType(profileSchema)
      )
      val authDF = spark.createDataFrame(
        sparkContext.parallelize(authData),
        StructType(authSchema)
      )
      val appDF = spark.createDataFrame(
        sparkContext.parallelize(appData),
        StructType(appSchema)
      )
      val preqDF = spark.createDataFrame(
        sparkContext.parallelize(preqData),
        StructType(preqSchema)
      )

      val results = SIIdentifyFoundersApplicationPrequalificationsProcessor.getFoundersInfo(appDF, preqDF, profileDF, authDF)

      assert(results.count() == 3)

      val row1 = results.filter(col("userId") === 1).first()
      assert(1 == row1.getAs[Int]("userId"))
      assert("test1@email.com".equals(row1.getAs[String]("traits.email")))
      assert(timestamp1.equals(row1.getAs[Timestamp]("traits.modified_at")))
    }
  }
