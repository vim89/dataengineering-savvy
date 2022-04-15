package com.vitthalmirji.dataengineering.datetime

import com.vitthalmirji.dataengineering.common.Helpers.extractMatching
import com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator
import org.apache.log4j.Logger
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe.{TypeTag, typeOf}

/**
 * Helper Singleton object for DateTime
 */
object DateTimeHelpers {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * [[com.vitthalmirji.dataengineering.datetime.DateTimeHelpers#getDatesBetweenAs(java.time.LocalDate, java.time.LocalDate, scala.Option, scala.reflect.api.TypeTags.TypeTag)]]
   * A generic function that returns (typed T) list of dates between given 2 dates of type [[java.time.LocalDate]]
   *
   * @param startDate          start date of the range
   * @param endDate            end date of the range
   * @param returnDatesPattern pattern in which detes must be returned
   * @param returnType         implicit TypeTag to ideintify  dataType at runtime
   * @tparam T Return type in which Set of dates are expected
   * @return Returns Set of generic typed dates
   *         Usage:
   *         val dates = getDatesBetweenAs[String](yearMonthDate"2021-07-01 Asia/Kolkata", yearMonthDate"2021-08-01 Asia/Kolkata")
   *         assert(dates.size.equals(32))
   *
   *         val dates2 = getDatesBetweenAs[String](yearMonthDate"2021-07-01 Asia/Kolkata", yearMonthDate"2021-07-01 Asia/Kolkata")
   *         assert(dates2.size.equals(1))
   */
  def getDatesBetweenAs[T](startDate: LocalDate, endDate: LocalDate, returnDatesPattern: Option[String] = None)(implicit returnType: TypeTag[T]): Set[T] = {
    if (startDate.isAfter(endDate)) {
      val errorMessage = s"startDate ${startDate.toString} is after endDate ${endDate.toString}"
      logger.error(errorMessage)
      throw new IllegalArgumentException(errorMessage)
    }
    val dates =
      startDate.toEpochDay.until(endDate.plusDays(1).toEpochDay).map(LocalDate.ofEpochDay).toSet
    returnType.tpe match {
      case t: Any if t =:= typeOf[String] =>
        dates
          .map(_.format(DateTimeFormatter.ofPattern(returnDatesPattern.getOrElse("yyyy-MM-dd"))))
          .asInstanceOf[Set[T]]
      case t: Any if t =:= typeOf[LocalDate] => dates.asInstanceOf[Set[T]]
      case _ => dates.asInstanceOf[Set[T]]
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.datetime.DateTimeHelpers#extractDate(java.lang.String, java.lang.String)]]
   * Internally uses [[com.vitthalmirji.dataengineering.common.Helpers#extractMatching(java.lang.String, java.lang.String)]]
   *
   * @param searchString String to extract date from
   * @param dateRegex    Regular expression to match date pattern for example: for yyyy-MM-dd: \\d{4}-\\d{2}-\\d{2}
   * @return Returns Option of matched/extracted string converted to [[java.time.LocalDate]] otherwise None
   */
  def extractDate(searchString: String, dateRegex: String): Option[LocalDate] = {
    val matched = extractMatching(searchString, dateRegex)
    logger.debug(s"Extracting date using regex $dateRegex from string $searchString; Matched result = $matched")
    matched.map(matchedDate => yearMonthDate"$matchedDate")
  }
}
