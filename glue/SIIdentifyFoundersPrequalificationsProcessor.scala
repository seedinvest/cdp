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

package com.circle.data.jobs

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import com.circle.data.glue.GlueJob
import com.circle.data.sources.DataLoader
import com.circle.data.utils.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Processor for SeedInvest identify founders with prequalifications.
 */
object SIIdentifyFoundersPrequalificationsProcessor
  extends GlueJob
  with DataLoader
  with Logging {

  def main(args: Array[String]): Unit = {

    implicit val context: GlueContext = new GlueContext(new SparkContext())
    implicit val sparkSession: SparkSession = context.sparkSession
    implicit val sqlContext: SQLContext = sparkSession.sqlContext

    logConfiguration()

    val options = initializeJob(context, args,
      Array(JobConfigFileParam, SourceGlueDatabaseParam, SourceGlueTableParam, SourceGlueTable2Param,
        SourceGlueTable3Param, SourceGlueTable4Param, SourceGlueTable5Param, OutputLocationParam)
    )

    val snapshotDatabase = options(SourceGlueDatabaseParam)
    val snapshotTable = options(SourceGlueTableParam)
    val snapshotTable2 = options(SourceGlueTable2Param)
    val snapshotTable3 = options(SourceGlueTable3Param)

    val outputFileLocation = options(OutputLocationParam)

    val athenaPrequalificationData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable)
    val userProfileData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable2)
    val authUserData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable3)

    athenaPrequalificationData.printSchema()
    userProfileData.printSchema()
    authUserData.printSchema()

    val result = getFoundersInfo(athenaPrequalificationData, userProfileData, authUserData)

    val timestampKey = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY/MM/dd_HHmmss"))
    val outputPath = s"$outputFileLocation/$timestampKey"

    result
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      // Use escape to sets a single character used for escaping quotes inside an already quoted value.
      .option("escape", "\"")
      .csv(outputPath)

    Job.commit()
  }

  /**
   * Get identify founders with application and prequalifications
   *
   * Example Presto Query:
   *
   * with prequalifications as (
   *  select
   *    preq.email,
   *    max(preq.created_at) as created_at,
   *    array_to_json(array_agg(row(
   *      preq.id as prequalification_id,
   *      preq.first_name,
   *      preq.last_name,
   *      preq.phone_number,
   *      preq.email,
   *      preq.venture_member_email,
   *      preq.created_at,
   *      preq.submitted_at,
   *      preq.approved_at,
   *      preq.rejected_at,
   *      preq.source_name,
   *      preq.source_medium,
   *      preq.state,
   *      preq.country_of_company_id,
   *      preq.company_legal_name,
   *      preq.company_name,
   *      preq.company_url,
   *      preq.title_at_company,
   *      preq.full_title_employees,
   *      preq.raise_amount,
   *      preq.raise_amount_currency,
   *      preq.latest_pitch_deck,
   *      preq.has_started_currenct_fundraise,
   *      preq.is_first_raise,
   *      preq.is_faked,
   *      preq.is_testing
   *    ))) as prequalification
   *  from athena_athenapplication app
   *  left outer join athena_athenaprequalification preq
   *    on app.prequalification_id = preq.id
   *  group by preq.email
   *  )
   * select
   *  coalesce(auth.id, profile.entity_ptr_id) as userID,
   *  auth.email as `traits.email`,
   *  appl.modified_at as `traits.modified_at`,
   *  appl.application as `traits.application`,
   *  appl.prequalification as `traits.prequalification`
   * from seedinvest_user_userprofile profile
   * inner join applications appl
   *  on profile.entity_ptr_id = appl.main_poc_id
   * left outer join auth_user auth
   *  on profile.user_id = auth.id
   * where preq.email is not null
   *
   * @return founders with application and prequalifications info
   */
  def getFoundersInfo(athenaPrequalificationData: DataFrame, userProfileData: DataFrame, authUserData: DataFrame): DataFrame = {
    var preq = athenaPrequalificationData.as("preq")
    preq = preq.withColumnRenamed("id", "prequalification_id")
    val preqJsonCols = struct("prequalification_id", "first_name", "last_name", "phone_number",
      "email", "venture_member_email", "created_at", "submitted_at", "approved_at", "rejected_at", "source_name",
      "source_medium", "state", "country_of_company_id", "company_legal_name", "company_name", "company_url",
      "title_at_company", "full_time_employees", "raise_amount", "raise_amount_currency", "latest_pitch_deck",
      "has_started_current_fundraise", "is_first_raise", "is_faked", "is_testing")
    preq = preq.withColumn("prequalification", to_json(preqJsonCols))

    val preq_json = preq
      .groupBy("email")
      .agg(
        max("created_at").as("created_at"),
        to_json(collect_list("prequalification")).as("prequalification_list")
      )

    preq = preq
      .join(preq_json, "email")
      .dropDuplicates()

    val preqCols = Array(
      "preq.email",
      "preq.created_at",
      "prequalification_list AS prequalification"
    )
    preq = preq.selectExpr(preqCols: _*)

    val result = userProfileData.as("profile")
      .join(authUserData.as("auth"), col("profile.user_id") === col("auth.id"), "left_outer")
      .join(preq.as("preq"), col("auth.email") === col("preq.email"), "inner")

    val selectCols = Array(
      "auth.id AS userId",
      "auth.email AS `traits.email`",
      "preq.created_at AS `traits.created_at`",
      "preq.prequalification AS `traits.prequalification`"
    )
    result
      .selectExpr(selectCols: _*)
      .filter("auth.email IS NOT NULL")
      .dropDuplicates()
  }
}

