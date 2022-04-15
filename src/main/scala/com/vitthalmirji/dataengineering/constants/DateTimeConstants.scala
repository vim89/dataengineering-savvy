package com.vitthalmirji.dataengineering.constants

import com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator
import java.util.TimeZone

/**
 * Constant values, Sealed classes for DateTime
 */
object DateTimeConstants {
  final val TZ_UTC: TimeZone = tz"UTC"
  final val TZ_CST: TimeZone = tz"US/Central"
  final val TZ_IST: TimeZone = tz"Asia/Kolkata"

  final val YEAR_MONTH_DATE_24HOUR_TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
  final val YEAR_MONTH_DATE_24HOUR_NANO_TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
  final val YEAR_MONTH_DATE_FORMAT = "yyyy-MM-dd"
}
