package com.vitthalmirji.dataengineering.datetime

import com.vitthalmirji.dataengineering.constants.DateTimeConstants.{YEAR_MONTH_DATE_24HOUR_NANO_TS_FORMAT, YEAR_MONTH_DATE_24HOUR_TS_FORMAT, YEAR_MONTH_DATE_FORMAT}

import java.time.{LocalDate, LocalDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.TimeZone

object DateTimeInterpolators {

  /** [[com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator]] for date & time given as string
   *
   * @param sc String Context for DateTime given as string
   */
  implicit class DateTimeInterpolator(sc: StringContext) {

    /**
     * [[com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator#yearMonthDate(scala.collection.Seq)]]
     * String GcsInterpolator for [[java.time.LocalDateTime]] parses to DateTime for given datetime in string
     *
     * @param args Date given as String
     * @return [[java.time.LocalDate]] object for given Date as string
     */
    def yearMonthDate(args: Any*): LocalDate =
      LocalDate.parse(sc.s(args: _*).trim, DateTimeFormatter.ofPattern(YEAR_MONTH_DATE_FORMAT))

    /** [[com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator#yearMonthDate24HrTs(scala.collection.Seq)]]
     * String GcsInterpolator for [[java.time.LocalDateTime]] parses to DateTime for given date time in string
     *
     * @param args Date & Timestamp given as String
     * @return [[java.time.LocalDateTime]] object for given Date & Timestamp as string
     */
    def yearMonthDate24HrTs(args: Any*): LocalDateTime =
      LocalDateTime.parse(sc.s(args: _*).trim, DateTimeFormatter.ofPattern(YEAR_MONTH_DATE_24HOUR_TS_FORMAT))

    /**
     * [[com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator#yearMonthDate24HrNanoTs(scala.collection.Seq)]]
     * String GcsInterpolator for String GcsInterpolator for [[java.time.LocalDateTime]] fetching Date Timestamp with Micro seconds
     *
     * @param args Date & Timestamp with Micro Seconds given as String
     * @return [[java.time.LocalDateTime]] object for given Date & Timestamp with 3 digit Nano Seconds as string
     */
    def yearMonthDate24HrNanoTs(args: Any*): LocalDateTime =
      LocalDateTime.parse(sc.s(args: _*).trim, DateTimeFormatter.ofPattern(YEAR_MONTH_DATE_24HOUR_NANO_TS_FORMAT))

    /**
     * [[com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator#yearMonthDate24HrNanoTsZone(scala.collection.Seq)]]
     * String GcsInterpolator for String GcsInterpolator for [[java.time.ZonedDateTime]] fetching Date Timestamp with Micro seconds
     *
     * @param args Date, Timestamp with Micro Seconds & Zone given as String
     * @return [[java.time.ZonedDateTime]] object for given Date & Timestamp with 3 digit Nano Seconds as string with Zone
     */
    def yearMonthDate24HrNanoTsZone(args: Any*): ZonedDateTime = {
      val tokens = sc.s(args: _*).trim.split(" ")
      ZonedDateTime.of(
        LocalDateTime.parse(
          s"${tokens.head} ${tokens(1)}",
          DateTimeFormatter.ofPattern(YEAR_MONTH_DATE_24HOUR_NANO_TS_FORMAT)
        ),
        tz"${tokens.last}".toZoneId
      )
    }

    /**
     * [[com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator#tz(scala.collection.Seq)]]
     * String GcsInterpolator for [[java.util.TimeZone]]
     * Refer timezones String constants supported here https://www.joda.org/joda-time/timezones.html
     *
     * @param args TimeZone string standard Refer timezones String Constants here https://www.joda.org/joda-time/timezones.html
     * @return
     */
    def tz(args: Any*): TimeZone = TimeZone.getTimeZone(sc.s(args: _*))
  }
}
