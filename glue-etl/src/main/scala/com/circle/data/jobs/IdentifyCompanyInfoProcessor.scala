package com.circle.data.jobs

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import com.circle.data.glue.{DataLoader, GlueJob}
import com.circle.data.utils.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

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
        SourceGlueTable3Param, SourceGlueTable4Param, SourceGlueTable5Param, OutputLocationParam)
    )

    val snapshotDatabase = options(SourceGlueDatabaseParam)
    val snapshotTable = options(SourceGlueTableParam)
    val outputFileLocation = options(OutputLocationParam)

    val companyData = getDataFrameForGlueCatalog(snapshotDatabase, snapshotTable)

    companyData.printSchema()

    val result = getCompanyInfo(companyData)

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
   * SELECT
   *  company.entity_ptr_id AS "groupId"
   *  , company.legal_name AS "traits.name"
   *  , company.phone_number AS "traits.phone"
   *  , company.company_entity_description AS "traits.description"
   *  FROM
   *  public_seedinvest_company_company company
   *  WHERE company.legal_name IS NOT NULL;
   *
   * @return basic user info
   */
  def getCompanyInfo(companyData: DataFrame): DataFrame = {
    val selectCols = Array(
      "company.entity_ptr_id as `groupId`",
      "company.legal_name AS `traits.name`",
      "company.phone_number AS `traits.phone`",
      "company.company_entity_description as `traits.description`"
    )
    companyData.as("company").selectExpr(selectCols: _*).where("company.legal_name IS NOT NULL")
  }
}
