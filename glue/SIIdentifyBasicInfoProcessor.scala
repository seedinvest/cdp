/*
 * Copyright (c) 2021 Circle Internet Financial Trading Company Limited.
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
 * Processor for SeedInvest identify basic user.
 */
object SIIdentifyBasicInfoProcessor
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
    val snapshotTable5 = options(SourceGlueTable5Param)
    val outputFileLocation = options(OutputLocationParam)

    val profileData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable)
    val authData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable2)
    val identityData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable3)
    val associationData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable4)
    val accreditationData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable5)

    profileData.printSchema()
    authData.printSchema()
    identityData.printSchema()
    associationData.printSchema()
    accreditationData.printSchema()

    val result = getBasicInfo(profileData, authData, identityData, associationData, accreditationData)

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
   * Get identify basic user.
   *
   * Example Presto Query:
   *
   * SELECT
   *   COALESCE(AUTH.ID,PROFILE.ENTITY_PTR_ID) AS "userId",
   *   AUTH.EMAIl AS "traits.email",
   *   UI.MODIFIED_AT AS "traits.modified_at",
   *   COALESCE(AUTH.DATE_JOINED, PROFILE.DATE_JOINED) AS "traits.date_joined",
   *   AUTH.LAST_LOGIN AS "traits.last_login",
   *   COALESCE(UI.FIRST_NAME,AUTH.FIRST_NAME) AS "traits.first_name",
   *   COALESCE(UI.LAST_NAME,AUTH.LAST_NAME) AS "traits.last_name",
   *   UI.PHONE_NUMBER AS "traits.phone_number",
   *   UI.COUNTRY_OF_CITIZENSHIP_ID AS "traits.country_of_citizenship_id",
   *   UI.DATE_OF_BIRTH AS "traits.date_of_birth",
   *   UI.ID AS "traits.user_identity_id",
   *   PROFILE.EMAIL_CONFIRMED AS "traits.email_confirmed",
   *   PROFILE.USER_ID AS "traits.user_id",
   *   PROFILE.USER_REQUEST_TO_BE_FORGOTTEN_EXPIRATION_DATE AS "traits.user_request_to_be_forgotten_expiration_date",
   *   PROFILE.USER_REQUEST_TO_BE_FORGOTTEN_NOTES AS "traits.user_request_to_be_forgotten_notes",
   *   PROFILE.USER_REQUESTED_TO_BE_FORGOTTEN_DATE AS "traits.user_requested_to_be_forgotten_date",
   *   UAS.ID AS "traits.user_accreditation_status_id",
   *   UAS.IS_ACCREDITED AS "traits.is_accredited",
   *   UAS.IS_ACCREDITED_VIA_ENTITY AS "traits.is_accredited_via_entity",
   *   UAS.IS_ACCREDITED_VIA_INCOME AS "traits.is_accredited_via_income",
   *   UAS.IS_ACCREDITED_VIA_FINRA_LICENSE AS "traits.is_accredited_via_finra_license",
   *   UAS.IS_ACCREDITED_VIA_TRUST AS "traits.is_accredited_via_trust",
   *   UAS.IS_ACCREDITED_VIA_NETWORTH AS "traits.is_accredited_via_networth",
   *   UAI.TITLE_AT_CURRENT_EMPLOYER AS "traits.title_at_current_employer",
   *   UAI.LAST_EMPLOYER_NAME AS "traits.last_employer_name",
   *   UAI.CURRENT_EMPLOYER_NAME AS "traits.current_employer_name",
   *   UAI.CURRENT_EMPLOYMENT_STATUS AS "traits.current_employment_status",
   *   UAI.ID AS "traits.user_association_information_id",
   *   UAI.STATE AS "traits.state"
   * FROM SEEDINVEST_USER_USERPROFILE PROFILE
   * LEFT OUTER JOIN AUTH_USER AUTH
   *   ON PROFILE.user_id = AUTH.id
   * LEFT OUTER JOIN SEEDINVEST_USER_IDENTITY UI
   *   ON UI.userprofile_id = PROFILE.entity_ptr_id
   * LEFT OUTER JOIN SEEDINVEST_USER_ASSOCIATION_INFORMATION UAI
   *   ON UI.userprofile_id = UAI.userprofile_id
   * LEFT OUTER JOIN seedinvest_user_accreditation_status UAS
   *   ON UI.userprofile_id = UAS.userprofile_id
   *
   * @return basic user info
   */
  def getBasicInfo(profileData: DataFrame, authData: DataFrame, identityData: DataFrame,
                       associationData: DataFrame, accreditationData: DataFrame): DataFrame = {
    val selectCols = Array(
      "COALESCE(auth.id, profile.entity_ptr_id) AS userId",
      "auth.email AS `traits.email`",
      "ui.modified_at AS `traits.modified_at`",
      "COALESCE(auth.date_joined, profile.date_joined) AS `traits.date_joined`",
      "auth.last_login AS `traits.last_login`",
      "COALESCE(ui.first_name, auth.first_name) AS `traits.first_name`",
      "COALESCE(ui.last_name, auth.last_name) AS `traits.last_name`",
      "ui.phone_number AS `traits.phone_number`",
      "ui.country_of_citizenship_id AS `traits.country_of_citizenship_id`",
      "ui.date_of_birth AS `traits.date_of_birth`",
      "ui.id AS `traits.user_identity_id`",
      "profile.email_confirmed AS `traits.email_confirmed`",
      "profile.user_id AS `traits.user_id`",
      "profile.user_request_to_be_forgotten_expiration_date AS `traits.user_request_to_be_forgotten_expiration_date`",
      "profile.user_request_to_be_forgotten_notes AS `traits.user_request_to_be_forgotten_notes`",
      "profile.user_requested_to_be_forgotten_date AS `traits.user_requested_to_be_forgotten_date`",
      "uas.id AS `traits.user_accreditation_status_id`",
      "uas.is_accredited AS `traits.is_accredited`",
      "uas.is_accredited_via_entity AS `traits.is_accredited_via_entity`",
      "uas.is_accredited_via_income AS `traits.is_accredited_via_income`",
      "uas.is_accredited_via_finra_license AS `traits.is_accredited_via_finra_license`",
      "uas.is_accredited_via_trust AS `traits.is_accredited_via_trust`",
      "uas.is_accredited_via_networth AS `traits.is_accredited_via_networth`",
      "uai.title_at_current_employer AS `traits.title_at_current_employer`",
      "uai.last_employer_name AS `traits.last_employer_name`",
      "uai.current_employment_status AS `traits.current_employer_status`",
      "uai.id AS `traits.user_association_information_id`",
      "uai.state AS `traits.state`"
    )

    val result = profileData.as("profile")
      .join(authData.as("auth"), col("profile.user_id") === col("auth.id"), "left_outer")
      .join(identityData.as("ui"), col("ui.userprofile_id") === col("profile.entity_ptr_id"), "left_outer")
      .join(associationData.as("uai"), col("ui.userprofile_id") === col("uai.userprofile_id"), "left_outer")
      .join(accreditationData.as("uas"), col("ui.userprofile_id") === col("uas.userprofile_id"), "left_outer")

    result.selectExpr(selectCols: _*)
  }
}
