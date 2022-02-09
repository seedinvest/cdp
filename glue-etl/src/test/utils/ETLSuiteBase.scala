package utils

/*
 * Copyright (c) 2022 Circle Internet Financial Trading Company Limited.
 * All rights reserved.
 *
 * Circle Internet Financial Trading Company Limited CONFIDENTIAL
 *
 * This repository includes unpublished proprietary source code of Circle Internet
 * Financial Trading Company Limited, Inc. The copyright notice above does not
 * evidence any actual or intended publication of such source code. Disclosure
 * of this source code or any related proprietary information is strictly
 * prohibited without the express written permission of Circle Internet Financial
 * Trading Company Limited.
 */

import com.amazonaws.services.glue.GlueContext
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

import java.util.TimeZone

/**
  * A base test suite that constructs a GlueContext that can be accessed by more than one test
  */
class ETLSuiteBase extends FunSuite
  with DataFrameSuiteBase {

  implicit var context: GlueContext = _
  implicit var sparkContext: SparkContext = _
  implicit var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {

    // Force UTC timezone. CI systems are already UTC by default, but this is useful for IDE debugging where timezone
    // is likely from dev system
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    // DataFrameSuiteBase sets up SparkContext so do that first, but only if it does not fail
    super.beforeAll()

    // GlueContext is created here because underlying SparkContext is
    // setup in super method
    context = new GlueContext(sc)
    sparkContext = sc
    sparkSession = context.sparkSession
  }

  def assertSameData(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collectAsList() === result.collectAsList())
  }

  def assertSame(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.schema === result.schema)
    assertSameData(expected, result)
  }

  def requireColumnsAbsent(df: DataFrame, cols: String*): Unit = {
    for (col <- cols) {
      assert(!df.columns.contains(col), s"should not have column '$col'")
    }
  }

  def requireColumnsPresent(df: DataFrame, cols: String*): Unit = {
    for (col <- cols) {
      assert(df.columns.contains(col), s"should have column '$col'")
    }
  }
}
