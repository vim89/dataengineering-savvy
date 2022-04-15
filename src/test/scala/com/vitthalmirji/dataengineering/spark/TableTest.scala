package com.vitthalmirji.dataengineering.spark

import com.vitthalmirji.dataengineering.datetime.DateTimeInterpolators.DateTimeInterpolator
import com.vitthalmirji.dataengineering.spark.Table.{ImplicitTablePartitionActions, deleteDfsLocation, getTableLocation, repairRefreshTable}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import com.vitthalmirji.dataengineering.spark.Table.TableDataframeActions

import java.nio.file.Paths
import scala.util.Try

class TableTest extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()

  override def beforeAll(): Unit = {
    val databases = Array("test_db")
    Try {
      databases.foreach(db => spark.sql(s"DROP DATABASE IF EXISTS $db").show())
      databases.foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db").show())
    }
  }

  test("getTableLocation") {
    spark.sql("DROP TABLE IF EXISTS test_db.office").show()
    spark.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS test_db.office(name STRING, city STRING, state STRING)
        |PARTITIONED BY(country STRING)
        |STORED AS ORC
        |LOCATION 'spark-warehouse/test_db.db/office'
        |""".stripMargin).show()

    val tableLocation: String = getTableLocation("test_db.office")
    val expectedLocation: String =
      s"file:${Paths.get("spark-warehouse", "test_db.db", "office").toFile.getAbsolutePath}"
    assert(tableLocation != null)
    assert(tableLocation.equals(expectedLocation))
    Try {
      getTableLocation("test_db.incorrect_table")
    }
  }

  test("drop & delete Partitions") {
    val op_cmpny_cd = "WMT-US"
    spark.sql("DROP TABLE IF EXISTS test_db.test_table_drop_partitions PURGE")
    spark.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table_drop_partitions (
        |id INT) PARTITIONED BY(bus_dt STRING, op_cmpny_cd STRING, week_nbr INT) STORED AS ORC
        |LOCATION 'spark-warehouse/test_db.db/test_table_drop_partitions'
        |""".stripMargin)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq((1, "2021-01-01", op_cmpny_cd, 1),
      (2, "2021-01-08", op_cmpny_cd, 2),
      (3, "2021-01-16", "WMT.COM", 3),
      (4, "2021-01-24", op_cmpny_cd, 4)
    ))).toDF("id", "bus_dt", "op_cmpny_cd", "week_nbr")
    df.write.mode("overwrite").insertInto("test_db.test_table_drop_partitions")

    val partitionsToDrop = List(
      Partition(Map("bus_dt" -> "2021-01-01")),
      Partition(Map("bus_dt" -> "2021-01-08")),
      Partition(Map("bus_dt" -> "2021-01-24", "op_cmpny_cd" -> "WMT-US", "week_nbr" -> 4))
    )

    partitionsToDrop.drop("test_db.test_table_drop_partitions")
    partitionsToDrop.delete("test_db.test_table_drop_partitions")

    val resultTable = spark.read.table("test_db.test_table_drop_partitions")
    resultTable.show()
    assert(resultTable.filter("week_nbr=4").count() < 1)
  }

  test("repairRefreshTable") {
    repairRefreshTable("test_db.test_table_drop_partitions")
    val table = spark.read.table("test_db.test_table_drop_partitions")
    table.show()
    assert(table.filter("week_nbr=4").count() < 1)
    repairRefreshTable("incorrect_table")
  }

  test("deleteDfsLocation") {
    assert(Try {
      deleteDfsLocation(s"${Paths.get("spark-warehouse", "test_db.db", "office").toFile.getAbsolutePath}")
    }.isSuccess)
    assert(Try {
      deleteDfsLocation(s"${Paths.get("spark-warehouse", "test_db.db", "incorrect").toFile.getAbsolutePath}")
    }.isSuccess)
    assert(Try {
      deleteDfsLocation(null)
    }.isFailure)
  }

  test("Partition case class unapply") {
    val partition = Partition(Map("bus_dt" -> "2022-01-01", "week_nbr" -> 4))
    Partition.unapply(partition).get should be(Map("bus_dt" -> "2022-01-01", "week_nbr" -> 4))
  }

  ignore("Get affected partition dates of table") {
    val affectedDates = spark
      .sql("SELECT * FROM ww_chnl_perf_app.chnl_perf_item_fact_dly WHERE op_cmpny_cd = 'WMT-US'")
      .getAffectedPartitionDates(since = yearMonthDate24HrTs"2022-04-03 00:00:00")
  }
}
