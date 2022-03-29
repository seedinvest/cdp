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
 * Processor for getting SeedInvest user who signed up.
 */
object InvestorInfoProcessor
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
    val investmentData = getDataFrameForGlueCatalog(snapshotDatabase, "public_investing_investment")

    val result = getInvestorData(sparkSession, authData, userProfileData, userIdentityData, investmentData)

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

  def getInvestorData(sparkSession: SparkSession, authData: DataFrame, userProfileData: DataFrame, userIdentityData: DataFrame, investmentData: DataFrame): DataFrame = {

    val selectCols = Array(
      "userprofile_id",
      "updated_date",
      "total_invested"
    )

    var result = investmentData
      .groupBy(col("userprofile_id"))
      .agg(
        max("modified_at").as("updated_date"),
        sum("amount").as("total_invested")
      )
      .selectExpr(selectCols: _*)

    result = result.filter(col("userprofile_id") === "1571")

    result
  }
}
