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
 * Processor for SeedInvest identify founders with application and prequalifications.
 */
object SIIdentifyFoundersApplicationPrequalificationsProcessor
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
    val snapshotTable4 = options(SourceGlueTable4Param)
    val outputFileLocation = options(OutputLocationParam)

    val athenaApplicationData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable)
    val athenaPrequalificationData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable2)
    val userProfileData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable3)
    val authUserData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable4)

    athenaApplicationData.printSchema()
    athenaPrequalificationData.printSchema()
    userProfileData.printSchema()
    authUserData.printSchema()

    val result = getFoundersInfo(athenaApplicationData, athenaPrequalificationData, userProfileData, authUserData)

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
   * with applications as (
   *  select
   *    app.main_poc_id,
   *    app.id as application_id,
   *    max(app.modified_at) as modified_at,
   *    array_to_json(array_agg(row(
   *      app.id as application_id,
   *      app.prequalification_id,
   *      app.main_poc.id,
   *      app.deal_id,
   *      app.created_at,
   *      app.submitted_at,
   *      app.approved_at,
   *      app.rejected_at,
   *      app.state,
   *      app.user_base_count,
   *      app.latest_pitch_deck,
   *      app.market_landscape_notes,
   *      app.preliminary_approval_notes,
   *      app.key_members_json,
   *      app.supplements_json,
   *      app.current_round_json
   *    ))) as application
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
   *  )
   *  group by app.main_poc_id, app.id
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
   *
   * @return founders with application and prequalifications info
   */
  def getFoundersInfo(athenaApplicationData: DataFrame, athenaPrequalificationData: DataFrame, userProfileData: DataFrame,
                      authUserData: DataFrame): DataFrame = {
    var app = athenaApplicationData.as("app")
    app = app.withColumnRenamed("id", "application_id")
    val appJsonCols = struct("application_id", "pre_qualification_id", "main_poc_id", "deal_id",
      "created_at", "submitted_at", "approved_at", "rejected_at", "state", "user_base_count", "latest_pitch_deck",
      "market_landscape_notes", "preliminary_approval_notes", "key_members_json", "supplements_json", "current_round_json")
    app = app.withColumn("application", to_json(appJsonCols))

    var app_json = app
      .groupBy("main_poc_id", "application_id")
      .agg(
        max("modified_at").as("modified_at"),
        to_json(collect_list("application")).as("application_list")
      )
    app_json = app_json.withColumnRenamed("application_id", "json_application_id")

    app = app.as("app")
      .join(app_json.as("app_json"),
        col("app.main_poc_id") === col("app_json.main_poc_id") &&
          col("app.application_id") === col("app_json.json_application_id"))
      .dropDuplicates()

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
        to_json(collect_list("prequalification")).as("prequalification_list")
      )

    preq = preq
      .join(preq_json.as("preq_json"), "email")
      .dropDuplicates()

    val applCols = Array(
      "app.main_poc_id",
      "application_id",
      "app.modified_at",
      "application_list as application",
      "prequalification_list as prequalification"
    )
    var applData = app.join(preq, col("app.pre_qualification_id") === col("prequalification_id"), "left_outer")
    applData = applData.selectExpr(applCols: _*)

    val result = userProfileData.as("profile")
      .join(applData.as("appl"), col("profile.entity_ptr_id") === col("appl.main_poc_id"), "inner")
      .join(authUserData.as("auth"), col("profile.user_id") === col("auth.id"), "left_outer")

    val selectCols = Array(
      "COALESCE(auth.id, profile.entity_ptr_id) AS userId",
      "auth.email AS `traits.email`",
      "appl.modified_at AS `traits.modified_at`",
      "appl.application AS `traits.application`",
      "appl.prequalification AS `traits.prequalification`"
    )
    result
      .selectExpr(selectCols: _*)
      .dropDuplicates()
  }
}
