package com.circle.data.glue

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.log4j.Logger

import scala.collection.JavaConverters.mapAsJavaMapConverter

trait GlueJob extends JobParameters with JobsErrorChecker {

  def initializeJob(context: GlueContext, args: Array[String], params: Array[String]): Map[String, String] = {
    val options = GlueArgParser.getResolvedOptions(args, params :+ JobNameParam)
    Job.init(options(JobNameParam), context, options.asJava)
    options
  }

  def logConfiguration()(implicit context: GlueContext, logger: Logger): Unit = {
    val glueString: String = context.getAllConfs.map(x => x._1 + "-->" + x._2).mkString("\n")
    val sparkString: String = context.sc.getConf.getAll.map(x => x._1 + "-->" + x._2).mkString("\n")
    logger.info(s"Glue Configuration:\n$glueString")
    logger.info(s"Spark Configuration:\n$sparkString")
  }
}
