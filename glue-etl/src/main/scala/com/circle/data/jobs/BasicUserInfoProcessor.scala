package com.circle.data.jobs

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import com.circle.data.glue.{DataLoader, GlueJob}
import com.circle.data.utils.Logging
import com.circle.data.utils.QueryBase._
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
    val accreditationStatusData = getDataFrameForGlueCatalog(snapshotDatabase, "public_seedinvest_user_accreditation_status")

    val filteredUserProfileData = userProfileData.filter(col("entity_ptr_id") > 650000 && col("entity_ptr_id") < 655000)
    val result = getBasicUserData(authData, filteredUserProfileData, accreditationStatusData, false)

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
}
