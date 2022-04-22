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

  /**
   * Get investor action data
   * SELECT
       profile.si_userprofile_id, 
       action.name,
       activity.*
     FROM crm_user_useractivity activity
       INNER JOIN crm_user_crmuserprofile profile ON activity.actor_id = profile.id
       INNER JOIN crm_user_eventaction action ON action.id = activity.event_action_id
   */
  def getInvestorActionData(
    userActivityData: DataFrame,
    userProfileData: DataFrame, 
    eventActionData: DataFrame
  ): DataFrame = {
    val selectCols = Array(
      "profile.si_userprofile_id AS `userId`",
      "action.name AS `actionName`",
      "activity.*" 
    )

    val result = userActivityData.as("activity")
      .join(userProfileData.as("profile"), col("profile.id") === col("activity.actor_id"))
      .join(eventActionData.as("action"), col("action.id") === col("activity.event_action_id"))
      .selectExpr(selectCols: _*)

    result
  }

  /**
   * Get SeedInvest investor email preference
   * SELECT
       rule.id,
       rule.created_at,
       rule.modified_at,
       rule.active AS subscribed,
       channel_contact_info AS email,
       preference.key,
       preference.label,
       preference.display_label
     FROM public_ponyexpress_preference_rule rule
       INNER JOIN public_ponyexpress_preferences_contact_channel channel
         ON rule.contact_channel_id=channel.id
       INNER JOIN public_ponyexpress_preferences_preference preference
         ON rule.preference_id=preference.id where active=true
   */
  def getEmailPreferenceData(
    preferenceRuleData: DataFrame,
    contactChannelData: DataFrame,
    preferenceData: DataFrame
  ): DataFrame = {
    val selectCols = Array(
      "rule.id",
      "rule.created_at",
      "rule.modified_at",
      "rule.active AS `subscribed`",
      "channel.contact_info AS `email`",
      "preference.key",
      "preference.label",
      "preference.display_label"
    )

    val result = preferenceRuleData.as("rule")
      .join(contactChannelData.as("channel"), col("channel.id") === col("rule.contact_channel_id"))
      .join(preferenceData.as("preference"), col("preference.id") === col("rule.preference_id"))
      .selectExpr(selectCols: _*)
      .filter(col("email").rlike("""^([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$"""))

    result
  }
}
