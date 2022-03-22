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
 * Processor for getting SeedInvest user who signed up.
 */
object BasicUserInfoProcessor
  extends GlueJob
    with DataLoader
    with Logging {

  def main(args: Array[String]): Unit = {

    implicit val context: GlueContext = new GlueContext(new SparkContext())
    implicit val sparkSession: SparkSession = context.sparkSession
    implicit val sqlContext: SQLContext = sparkSession.sqlContext

    logConfiguration()

    val options = initializeJob(context, args,
      Array(SourceGlueDatabaseParam, OutputLocationParam)
    )

    val snapshotDatabase = options(SourceGlueDatabaseParam)
    val outputFileLocation = options(OutputLocationParam)

    val authData = getDataFrameForGlueCatalog(snapshotDatabase,  "public_auth_user")
    val userProfileData = getDataFrameForGlueCatalog(snapshotDatabase, "public_seedinvest_user_userprofile")
    val userIdentityData = getDataFrameForGlueCatalog(snapshotDatabase, "public_seedinvest_user_identity")

    val result = getBasicUserData(authData, userProfileData, userIdentityData)

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
      SELECT DISTINCT
      profile.entity_ptr_id AS "userId"
      ,COALESCE(ui.first_name, auth.first_name) AS "traits.first_name"
      ,COALESCE(ui.last_name, auth.last_name) AS "traits.last_name"
      ,auth.email AS "traits.email"
      ,ui.phone_number AS "traits.phone"
      ,profile.email_confirmed AS "traits.email_confirmed"
      ,profile.date_joined AS "timestamp"
      FROM auth_user auth
      INNER JOIN seedinvest_user_userprofile profile ON profile.user_id = auth.id
      INNER JOIN seedinvest_user_identity ui ON ui.userprofile_id = profile.entity_ptr_id
   *
   * @return basic user info
   */
  def getBasicUserData(authData: DataFrame, userProfileData: DataFrame, userIdentityData: DataFrame): DataFrame = {
    val selectCols = Array(
      "profile.entity_ptr_id AS `user_id`",
      "COALESCE(ui.first_name, auth.first_name) AS `traits.first_name`",
      "COALESCE(ui.last_name, auth.last_name) AS `traits.last_name`",
      "auth.email AS `traits.email`",
      "ui.phone_number AS `traits.phone`",
      "profile.email_confirmed AS `traits.email_confirmed`",
      "profile.date_joined AS `timestamp`"
    )

    authData.as("auth")
      .join(userProfileData.as("profile"), col("profile.entity_ptr_id") === col("auth.id"))
      .join(userIdentityData.as("ui"), col("ui.userprofile_id") === col("profile.entity_ptr_id"))
      .selectExpr(selectCols: _*)
  }
}
