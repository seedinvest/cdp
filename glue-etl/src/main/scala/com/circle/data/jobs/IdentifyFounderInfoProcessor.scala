package com.circle.data.jobs

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import com.circle.data.glue.{DataLoader, GlueJob}
import com.circle.data.utils.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Processor for getting SeedInvest founder information.
 */
object IdentifyFounderInfoProcessor
  extends GlueJob
    with DataLoader
    with Logging {

  def main(args: Array[String]): Unit = {

    implicit val context: GlueContext = new GlueContext(new SparkContext())
    implicit val sparkSession: SparkSession = context.sparkSession
    implicit val sqlContext: SQLContext = sparkSession.sqlContext

    logConfiguration()

    val options = initializeJob(context, args,
      Array(SourceGlueDatabaseParam, SourceGlueTableParam, SourceGlueTable2Param,
        SourceGlueTable3Param, SourceGlueTable4Param, SourceGlueTable5Param, OutputLocationParam)
    )

    val snapshotDatabase = options(SourceGlueDatabaseParam)
    val snapshotTable = options(SourceGlueTableParam)
    val snapshotTable2 = options(SourceGlueTable2Param)
    val snapshotTable3 = options(SourceGlueTable3Param)
    val snapshotTable4 = options(SourceGlueTable4Param)
    val outputFileLocation = options(OutputLocationParam)

    val profileData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable)
    val applicationsData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable2)
    val authData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable3)
    val identityData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable4)

    val result = getFounderInfo(applicationsData, profileData, authData, identityData)

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
   *  Example Presto Query:
   *
      WITH applications AS (
        SELECT
        appl.main_poc_id
        FROM public_athena_athenaapplication appl
        GROUP BY 1
      )
      SELECT DISTINCT
      COALESCE(auth.id, profile.entity_ptr_id) AS "userId"
      , COALESCE(ui.first_name, auth.first_name) AS "traits.first_name"
      , COALESCE(ui.last_name, auth.last_name) AS "traits.last_name"
      , auth.email AS "traits.email"
      , ui.phone_number AS "traits.phone"
      , profile.email_confirmed AS "traits.email_confirmed"
      FROM applications
      INNER JOIN public_seedinvest_user_userprofile profile ON profile.entity_ptr_id = applications.main_poc_id
      INNER JOIN public_auth_user auth ON auth.id = profile.user_id
      INNER JOIN public_seedinvest_user_identity ui ON ui.userprofile_id = profile.entity_ptr_id
   *
   * @return basic user info
   */
  def getFounderInfo(applicationsData: DataFrame, profileData: DataFrame, authData: DataFrame,
                     identityData: DataFrame): DataFrame = {
    val idSelCols = Array("appl.main_poc_id", "ROW_NUMBER() OVER (PARTITION BY appl.main_poc_id order by appl.main_poc_id) as row_number")
    var apps = applicationsData.as("appl").selectExpr(idSelCols: _*)
    val selectCols = Array("COALESCE(auth.id, profile.entity_ptr_id) AS userId",
      "COALESCE(ui.first_name, auth.first_name) AS `traits.first_name`",
      "COALESCE(ui.last_name, auth.last_name) AS `traits.last_name`",
      "auth.email AS `traits.email`", "ui.phone_number AS `traits.phone`",
      "profile.email_confirmed AS `traits.email_confirmed`"
    )
    apps = apps.as("applications")
      .join(profileData.as("profile"), col("profile.entity_ptr_id") === col("applications.main_poc_id"))
      .join(authData.as("auth"), col("auth.id") === col ("profile.user_id"))
      .join(identityData.as("ui"), col("ui.userprofile_id") === col("profile.entity_ptr_id"))
    apps.selectExpr(selectCols: _*)
  }
}
