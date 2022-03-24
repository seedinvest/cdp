/*
 * Copyright (c) 2019 Circle Internet Financial Trading Company Limited.
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

package com.circle.data.utils


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, OffsetDateTime, YearMonth, ZoneId, ZoneOffset}
import java.util.{Calendar, Date}

object DateUtils {

  def nowUTC(): OffsetDateTime = {
    OffsetDateTime.now(ZoneOffset.UTC)
  }

  protected val isoDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")


  // A simple date formatter.
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateOnlyFormatter = new SimpleDateFormat("yyyy-MM-dd")


  def iso8601Date(date: OffsetDateTime): String = {
    date.format(isoDateFormat)
  }

  def secondOfDay(date: OffsetDateTime): Long = {
    date.toLocalTime.toSecondOfDay
  }

  def compactDateFormat(date: OffsetDateTime): Int = {
    f"${date.getYear}%d${date.getMonthValue}%02d${date.getDayOfMonth}%02d".toInt
  }

  def min(left: Timestamp, right: Timestamp): Timestamp = {
    assert(left != null, "left must not be null")
    assert(right != null, "right must not be null")

    left.compareTo(right) match {
      case -1 => left
      case 0  => left
      case 1  => right
    }
  }

  /** Get an integer representing the given date in the format YYYYMMDD
    *
    * @param date an OffsetDateTime
    */
  val getDT = (date: OffsetDateTime) => {
    date.getYear * 10000 +
      date.getMonthValue * 100 +
      date.getDayOfMonth
  }

  /**
   * Truncate a Timestamp to the beginning of the date.
   */
  val truncateBeginningOfDate = (date : Date) => {
    org.apache.commons.lang.time.DateUtils.truncate(date, Calendar.DATE)
  }

  /**
   * Subtract the numDays from a given date.
   */
  val minusDays = (date : Date, numDays : Int) => {
    val MILLIS_IN_A_DAY : Long = 1000 * 60 * 60 * 24
    val result = new Date(date.getTime - numDays * MILLIS_IN_A_DAY)
    result
  }

  def dateToYearMonth(date: Date) : YearMonth = {
    val result = YearMonth.from(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
    result
  }

  def localDateToDate(localDate: LocalDate) : Date = {
    val result = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant())
    result
  }

  /**
   * First day of last month
   */
  def firstDayofLastMonth(date: Date): Date = {
    val yearMonthNow = dateToYearMonth(date)
    val yearMonthPrevious = yearMonthNow.minusMonths(1)
    val fd = yearMonthPrevious.atDay(1)
    val fdDate = localDateToDate(fd)
    fdDate
  }

  /**
   * First day of current month
   */
  def firstDayofCurrentMonth(date: Date): Date = {
    val yearMonthNow = dateToYearMonth(date)
    val fd = yearMonthNow.atDay(1)
    val fdDate = localDateToDate(fd)
    fdDate
  }

  /**
   * Last day of last month
   */
  def lastDayofLastMonth(date: Date): Date = {
    val yearMonthNow = dateToYearMonth(date)
    val yearMonthPrevious = yearMonthNow.minusMonths(1)
    val ld = yearMonthPrevious.atEndOfMonth()
    val ldDate = localDateToDate(ld)
    ldDate
  }

  /**
   * Difference between two given dates inclusive of start date and end date
   */
  def differenceBetweenDays(startDate: String,endDate: String) : Long = {
    val startDates=dateOnlyFormatter.parse(startDate)
    val endDates=dateOnlyFormatter.parse(endDate)
    val ms =endDates.getTime()-startDates.getTime()
    var days=(ms / (60 * 60 * 24 * 1000))
    days=days.toInt
    days+1
  }
}