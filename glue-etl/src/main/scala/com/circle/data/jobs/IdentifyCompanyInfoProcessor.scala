package com.circle.data.jobs

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import com.circle.data.glue.{DataLoader, GlueJob}
import com.circle.data.utils.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Processor for getting SeedInvest founder information.
 */
object IdentifyCompanyInfoProcessor
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
        SourceGlueTable3Param, SourceGlueTable4Param, SourceGlueTable5Param, SourceGlueTable6Param,
        SourceGlueTable7Param, SourceGlueTable8Param, SourceGlueTable9Param, SourceGlueTable10Param, OutputLocationParam)
    )

    val snapshotDatabase = options(SourceGlueDatabaseParam)

    val snapshotTable = options(SourceGlueTableParam)
    val snapshotTable2 = options(SourceGlueTable2Param)
    val snapshotTable3 = options(SourceGlueTable3Param)
    val snapshotTable4 = options(SourceGlueTable4Param)
    val snapshotTable5 = options(SourceGlueTable5Param)
    val snapshotTable6 = options(SourceGlueTable6Param)
    val snapshotTable7 = options(SourceGlueTable7Param)
    val snapshotTable8 = options(SourceGlueTable8Param)
    val snapshotTable9 = options(SourceGlueTable9Param)
    val snapshotTable10 = options(SourceGlueTable10Param)

    val outputFileLocation = options(OutputLocationParam)

    val companyData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable)
    val fundingData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable2)
    val appData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable3)
    val dealData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable4)
    val campaignDealData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable5)
    val campaignData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable6)
    val fundStageData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable7)
    val fundDraftData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable8)
    val profileData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable9)
    val authData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable10)

    val result = getCompanyInfo(companyData, fundingData, appData, dealData, campaignDealData, campaignData, fundStageData, fundDraftData, profileData, authData)

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
  * Get identify company information.
  *
  * Example Presto Query:
  * SELECT DISTINCT
  * COALESCE(auth.id, profile.entity_ptr_id) AS "userId"
  * , company.entity_ptr_id AS "groupId"
  * , company.legal_name AS "traits.name"
  * , company.phone_number AS "traits.phone"
  * , company.company_entity_description AS "traits.description"
  * , draft.company_url AS "traits.website"
  * FROM seedinvest_company_company company
  * INNER JOIN funding_round_fundinground funding ON funding.company_id = company.entity_ptr_id
  * INNER JOIN athena_athenaapplication app ON app.main_poc_id = company.main_poc_id
  * INNER JOIN seedinvest_deal_deal deal ON deal.funding_round_id = funding.id
  * INNER JOIN seedinvest_fundraising_campaign_deals fclink ON fclink.deal_id = deal.id
  * INNER JOIN seedinvest_fundraising_campaign campaign ON fclink.fundraisingcampaign_id = campaign.id
  * INNER JOIN seedinvest_fundraising_profile_stage fps ON fps.fundraising_campaign_id = campaign.id
  * INNER JOIN seedinvest_fundraising_profile_draft draft ON draft.stage_id = fps.id
  * INNER JOIN seedinvest_user_userprofile profile ON profile.entity_ptr_id = app.main_poc_id
  * LEFT JOIN auth_user auth ON auth.id = profile.user_id
  * WHERE company.legal_name IS NOT NULL;
  *
  * @return basic company info
  */
  def getCompanyInfo(
    companyData: DataFrame,
    fundingData: DataFrame,
    appData: DataFrame,
    dealData: DataFrame,
    campaignDealData: DataFrame,
    campaignData: DataFrame,
    fundStageData: DataFrame,
    fundDraftData: DataFrame,
    profileData: DataFrame,
    authData: DataFrame
  ): DataFrame = {
    val selectCols = Array(
      "COALESCE(auth.id, profile.entity_ptr_id) AS `userId`",
      "company.entity_ptr_id AS `groupId`",
      "company.legal_name AS `traits.name`",
      "company.phone_number AS `traits.phone`",
      "company.company_entity_description AS `traits.description`",
      "draft.company_url AS `traits.website`"
    )
    companyData.as("company")
      .join(fundingData.as("funding"), col("funding.company_id") === col("company.entity_ptr_id"))
      .join(appData.as("app"), col("app.main_poc_id") === col("company.main_poc_id"))
      .join(dealData.as("deal"), col("deal.funding_round_id") === col("funding.id"))
      .join(campaignDealData.as("fclink"), col("fclink.deal_id") === col("deal.id"))
      .join(campaignData.as("campaign"), col("fclink.fundraisingcampaign_id") === col("campaign.id"))
      .join(fundStageData.as("fps"), col("fps.fundraising_campaign_id") === col("campaign.id"))
      .join(fundDraftData.as("draft"), col("draft.stage_id") === col("fps.id"))
      .join(profileData.as("profile"), col("profile.entity_ptr_id") === col ("app.main_poc_id"))
      .join(authData.as("auth"), col("auth.id") === col ("profile.user_id"))
      .selectExpr(selectCols: _*).where("company.legal_name IS NOT NULL")
      .distinct()
  }
}
