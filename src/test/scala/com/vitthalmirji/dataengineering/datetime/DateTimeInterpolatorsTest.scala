package com.vitthalmirji.dataengineering.datetime

import com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, LocalDateTime, ZonedDateTime}
import java.util.TimeZone

class DateTimeInterpolatorsTest extends AnyFunSuite with BeforeAndAfterAll {

  test("tz TimeZone") {
    val IST_TIME_ZONE = tz"Asia/Kolkata"
    assert(IST_TIME_ZONE.isInstanceOf[TimeZone])
  }

  test("yearMonthDate") {
    val date = yearMonthDate"2021-07-01"
    assert(date.isInstanceOf[LocalDate])
    assert(date.getYear.equals(2021))
    assert(date.getMonthValue.equals(7))
    assert(date.getDayOfMonth.equals(1))
  }

  test("yearMonthDate24HrTs") {
    val date = yearMonthDate24HrTs"2021-08-21 20:49:50"
    assert(date.isInstanceOf[LocalDateTime])
    assert(date.getYear.equals(2021))
    assert(date.getMonthValue.equals(8))
    assert(date.getDayOfMonth.equals(21))
    assert(date.getHour.equals(20))
    assert(date.getMinute.equals(49))
    assert(date.getSecond.equals(50))
  }

  test("yearMonthDate24HrNanoTs") {
    val date = yearMonthDate24HrNanoTs"2021-08-21 20:49:50.987"
    assert(date.isInstanceOf[LocalDateTime])
    assert(date.getYear.equals(2021))
    assert(date.getMonthValue.equals(8))
    assert(date.getDayOfMonth.equals(21))
    assert(date.getHour.equals(20))
    assert(date.getMinute.equals(49))
    assert(date.getSecond.equals(50))
    assert(date.getNano.equals(987000000))
  }

  test("yearMonthDate24HrNanoTsZone") {
    val date = yearMonthDate24HrNanoTsZone"2021-08-21 20:49:50.123 Asia/Kolkata"
    assert(date.isInstanceOf[ZonedDateTime])
    assert(date.getYear.equals(2021))
    assert(date.getMonthValue.equals(8))
    assert(date.getDayOfMonth.equals(21))
    assert(date.getHour.equals(20))
    assert(date.getMinute.equals(49))
    assert(date.getSecond.equals(50))
    assert(date.getNano.equals(123000000))
    assert(date.getZone.toString.equals("Asia/Kolkata"))
  }

  override protected def afterAll(): Unit = {}
}
