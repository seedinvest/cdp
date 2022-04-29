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
 * Processor for getting SeedInvest user's email preference
 */
object EmailPreferenceProcessor
  extends GlueJob with DataLoader with Logging {

  def main(args: Array[String]): Unit = {

    implicit val context: GlueContext = new GlueContext(new SparkContext())
    implicit val sparkSession: SparkSession = context.sparkSession
    implicit val sqlContext: SQLContext = sparkSession.sqlContext

    logConfiguration()

    val options = initializeJob(context, args,
      Array(OutputLocationParam)
    )

    val outputFileLocation = options(OutputLocationParam)

    val authData = getDataFrameForGlueCatalog(databaseTable = "siservices_prod_db.public_auth_user")
    val profileData = getDataFrameForGlueCatalog(databaseTable = "siservices_prod_db.public_seedinvest_user_userprofile")
    val preferenceRuleData = getDataFrameForGlueCatalog(databaseTable = "investorcrm_db.public_ponyexpress_preference_rule")
    val contactChannelData = getDataFrameForGlueCatalog(databaseTable = "investorcrm_db.public_ponyexpress_preferences_contact_channel")
    val preferenceData = getDataFrameForGlueCatalog(databaseTable = "investorcrm_db.public_ponyexpress_preferences_preference")

    val result = getEmailPreferenceData(
      authData = authData,
      profileData = profileData,
      preferenceRuleData = preferenceRuleData,
      contactChannelData = contactChannelData,
      preferenceData = preferenceData
    )

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
