package com.circle.data.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import java.sql.Timestamp
import java.util.Date

object QueryBase {
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

  def getBasicUserData(authData: DataFrame, userProfileData: DataFrame, userIdentityData: DataFrame, recentOnly: Boolean = false): DataFrame = {
    val selectCols = Array(
      "profile.user_id AS `userId`",
      "COALESCE(ui.first_name, auth.first_name) AS `traits.first_name`",
      "COALESCE(ui.last_name, auth.last_name) AS `traits.last_name`",
      "auth.email AS `traits.email`",
      "auth.last_login AS `traits.last_login`",
      "ui.phone_number AS `traits.phone`",
      "profile.email_confirmed AS `traits.email_confirmed`",
      "profile.date_joined AS `timestamp`"
    )

    var result = authData.as("auth")
      .join(userProfileData.as("profile"), col("profile.user_id") === col("auth.id"))
      .join(userIdentityData.as("ui"), col("ui.userprofile_id") === col("profile.entity_ptr_id"))
      .selectExpr(selectCols: _*)

      if (recentOnly) {
        val startTimestampLocal = new Timestamp(DateUtils.minusDays(new Date(), 2).getTime)
        result = result.filter(col("auth.last_login").geq(startTimestampLocal))
      }

      result
  }
}