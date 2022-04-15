package com.vitthalmirji.dataengineering.datetime

import com.vitthalmirji.dataengineering.datetime.DateTimeHelpers.getDatesBetweenAs
import com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DateTimeHelpersTest extends AnyFunSuite with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {}

  test("getDatesBetween") {
    val dates = getDatesBetweenAs[String](yearMonthDate"2021-07-01", yearMonthDate"2021-08-01")
    assert(dates.size.equals(32))

    val dates2 = getDatesBetweenAs[String](yearMonthDate"2021-07-01", yearMonthDate"2021-07-01")
    assert(dates2.size.equals(1))
  }

  override protected def afterAll(): Unit = {}
}
