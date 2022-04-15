package com.vitthalmirji.dataengineering.common

import com.vitthalmirji.dataengineering.common.Helpers.{extractMatching, trySafely}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HelpersTest extends AnyFunSuite with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
  }

  test("trySafely Wrapper") {
    val tryResult = trySafely({
      1 / 0
    }, errorMessage = Some("Cannot divide by zero"), exceptionHandlingCodeblock = Some({
      "Exception Handling Code Block"
    })
    )

    assert(tryResult.isLeft)
    assert(!tryResult.isRight)
    if (tryResult.isRight) {
      println(tryResult.right.get)
    } else {
      println(tryResult.left.get)
      assert(tryResult.left.get.equals("Exception Handling Code Block"))
    }
  }

  test("extractMatching") {
    val path = "gs://ad_chnl_perf_ds_store/ww_chnl_perf_app.db/chnl_perf_item_fact_dly/bus_dt=2020-01-01"
    val regex = "[a-zA-Z0-9\\_]*\\.db"
    val database = extractMatching(path, regex)
    assert(database.isDefined)
    assert(database.getOrElse("invalid").equals("ww_chnl_perf_app.db"))
  }

  override protected def afterAll(): Unit = {}
}
