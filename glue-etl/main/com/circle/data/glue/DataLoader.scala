package com.circle.data.glue

import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.circle.data.utils.Logging
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

trait DataLoader extends Logging {

  object SourceTypes {
    val csv: String = "csv"
    val json: String = "json"
    val parquet: String = "parquet"
  }

  val sourceOptions: String =
    "{\"paths\":[\"%s\"], \"recurse\":true}"


  def getDataFrameForSourcePath(sourcePath: String, sourceFormat: String = SourceTypes.parquet, transformationContext: String = null)
                               (implicit context: GlueContext, logger: Logger): DataFrame = {
    if (sourcePath.startsWith("s3")) {
      getDynamicFrameForS3Source(sourcePath, sourceFormat, transformationContext).toDF()
    } else {
      logger.info(s"Loading data for: '$sourcePath'")
      sourceFormat match {
        case c if c == SourceTypes.csv => context.read.csv(sourcePath).toDF()
        case p if p == SourceTypes.parquet => context.read.parquet(sourcePath).toDF()
        case j if j == SourceTypes.json => context.read.json(sourcePath).toDF()
      }
    }
  }

  def getDynamicFrameForS3Source(sourcePath: String, sourceFormat: String = SourceTypes.parquet, transformationContext: String = null,
                                 formatOptions: JsonOptions = JsonOptions.empty)
                                (implicit context: GlueContext): DynamicFrame = {
    logger.info(
      s"""Loading data for:
         | source path: $sourcePath
         | source format: $sourceFormat
         | transformation context: $transformationContext""".stripMargin)

    context.getSourceWithFormat(
      connectionType = "s3",
      options = JsonOptions(sourceOptions.format(sourcePath)),
      transformationContext = Option(transformationContext).getOrElse(""),
      format = sourceFormat,
      formatOptions = formatOptions).getDynamicFrame()
  }

  def getDataFrameForGlueCatalog(database: String, table: String, transformationContext: String = "loading source data")
                                (implicit context: GlueContext): DataFrame = {
    getDynamicFrameForGlueCatalog(database, table, transformationContext).toDF()
  }

  def getDynamicFrameForGlueCatalog(database: String, table: String, transformationContext: String = "loading source data")
                                   (implicit context: GlueContext): DynamicFrame = {
    context.getCatalogSource(
      database = database,
      tableName = table,
      transformationContext = transformationContext).getDynamicFrame()
  }

}
