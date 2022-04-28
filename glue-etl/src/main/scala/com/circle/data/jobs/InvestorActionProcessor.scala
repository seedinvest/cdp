package com.circle.data.jobs

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import com.circle.data.glue.{DataLoader, GlueJob}
import com.circle.data.utils.Logging
import com.circle.data.utils.QueryBase._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

import java.time.LocalDateTime
import java.util.Date
import java.time.format.DateTimeFormatter

/**
 * Processor for getting SeedInvest user actions
 */
object InvestorActionProcessor
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

    val userActivityData = getDataFrameForGlueCatalog(snapshotDatabase, "public_crm_user_useractivity")
    val userProfileData = getDataFrameForGlueCatalog(snapshotDatabase, "public_crm_user_crmuserprofile")
    val eventActionData = getDataFrameForGlueCatalog(snapshotDatabase, "public_crm_user_eventaction")

    /* Filter out the following events.
      7:  DEAL_OUTREACH_RESPONSE_EVENT
      8:  DEAL_OUTREACH_RESPONSE_FINAL_OUTCOME
      10: USER_ADDED_TO_BASE_PIPELINE
      12: USER_COMPLETED_APPLICATION
      13: USER_COMPLETED_LIVE_EVENT_PREREGISTRATION
      32: USER_STARTED_AUTOINVEST_MEMBERSHIP_ONBOARDING
      34: USER_SUBSCRIBED_TO_LIVE_EVENT_COMPANY
      35: USER_SUBSCRIBED_TO_LIVE_EVENT_NOTIFICATIONS
      36: USER_TARGETED_IN_CAMPAIGN
     */
    val filteredEventActionData = eventActionData.filter(!(col("id") isin (7, 8, 10, 12, 13, 32, 34, 35, 36)))

    val result = getInvestorActionData(userActivityData, userProfileData, filteredEventActionData)

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
