package com.vitthalmirji.dataengineering.spark

import com.vitthalmirji.dataengineering.constants.StringConstants.AUDIT_COLUMN_NAMES
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.vitthalmirji.dataengineering.spark.ETL.ETLDataframeActions
import com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations
import com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions

class ETLTest extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()

  override def beforeAll(): Unit = {
    try {
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict").show()
      val databases = List("source", "ww_chnl_perf_stg", "ww_chnl_perf_app")
      databases.foreach(db => spark.sql(s"DROP DATABASE IF EXISTS $db").show())
      databases.foreach(db => spark.sql(s"CREATE DATABASE $db").show())
    } catch {
      case _: Throwable =>
    }
    spark
      .sql(
        """
          |CREATE EXTERNAL TABLE IF NOT EXISTS ww_chnl_perf_app.test (
          |id INT, name STRING, age INT, userid STRING, load_ts TIMESTAMP, upd_ts TIMESTAMP
          |) PARTITIONED BY(bus_dt DATE, op_cmpny_cd STRING, delta_flag STRING) STORED AS ORC
          |LOCATION 'spark-warehouse/ww_chnl_perf_app.db/test'
          |""".stripMargin
      )
      .show()

    spark
      .sql(
        """
          |CREATE EXTERNAL TABLE IF NOT EXISTS ww_chnl_perf_stg.test_bkp (
          |id INT, name STRING, age INT, userid STRING, load_ts TIMESTAMP, upd_ts TIMESTAMP
          |) PARTITIONED BY(bus_dt DATE, op_cmpny_cd STRING, delta_flag STRING) STORED AS ORC
          |LOCATION 'spark-warehouse/ww_chnl_perf_stg.db/test_bkp'
          |""".stripMargin
      )
      .show()

    spark
      .sql(
        """
          |CREATE EXTERNAL TABLE IF NOT EXISTS source.test_source (
          |id INT, name STRING, age INT, op_cmpny_cd STRING, bus_dt STRING
          |) STORED AS ORC
          |LOCATION 'spark-warehouse/source.db/test_source'
          |""".stripMargin
      )
      .show()

    spark.read.format("csv").options(Map("header" -> "false"))
      .load("src/test/resources/data/test/test_source.csv")
      .write.format("orc").mode("overwrite").insertInto("source.test_source")
  }

  test("Initial load Delta Calculation - Insert Only") {
    val sourceQuery =
      "SELECT t1.* FROM source.test_source t1"
    val extractDf = spark.sql(sourceQuery).mutateAuditColumns
    val deltaDf = extractDf.performDelta(previousLoadedDataset = None, primaryKeys = List("id", "bus_dt"), columnsExcludeHashcodeCalculation = None)
    deltaDf.show(false)
    deltaDf
      .select(spark.table("ww_chnl_perf_app.test").columns.map(col): _*)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto("ww_chnl_perf_app.test")

    val validateDf = spark.sql("SELECT DISTINCT delta_flag FROM ww_chnl_perf_app.test")
    validateDf.show(false)
    assert(validateDf.count().equals(1L))
    assert(validateDf.take(1)(0).getAs[String]("delta_flag").equals("I"))
  }

  test("Subsequent loads Delta calculation") {
    val sourceQuery =
      """SELECT t1.id, t1.name, t1.op_cmpny_cd, t1.bus_dt,
        |CASE WHEN t1.name = 'Christian' THEN 31 ELSE t1.age END AS age
        |FROM source.test_source t1
        |WHERE t1.name <> 'Bob'
        |UNION ALL
        |SELECT 10 AS id, 'Ryan' AS name, 'WMT-US' AS op_cmpny_cd, '2021-01-11' AS bus_dt,
        |29 AS age
        |""".stripMargin
    val extractDf = spark.sql(sourceQuery).mutateAuditColumns

    extractDf.show()

    val targetTable = spark.table("ww_chnl_perf_app.test")

    val partitionsToDrop = targetTable
      .selectExpr("CAST (bus_dt AS STRING) AS bus_dt")
      .distinct()
      .take(100)
      .map(_.getAs[String]("bus_dt"))
      .map(dt => Partition(Map("bus_dt" -> dt)))
      .toList

    partitionsToDrop.drop("ww_chnl_perf_stg.test_bkp")
    partitionsToDrop.delete("ww_chnl_perf_stg.test_bkp")

    targetTable.write.mode("overwrite").format("orc").insertInto("ww_chnl_perf_stg.test_bkp")
    val previousLoadedDf = spark.table("ww_chnl_perf_stg.test_bkp")

    previousLoadedDf.show()

    assert(previousLoadedDf.count() > 0)

    val deltaDf = extractDf.performDelta(
      previousLoadedDataset = Some(previousLoadedDf),
      primaryKeys = List("id", "bus_dt"),
      columnsExcludeHashcodeCalculation = None
    )
    deltaDf.show(false)

    partitionsToDrop.drop("ww_chnl_perf_app.test")
    partitionsToDrop.delete("ww_chnl_perf_app.test")

    deltaDf
      .select(spark.table("ww_chnl_perf_app.test").columns.map(col): _*)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto("ww_chnl_perf_app.test")

    val validateDf = spark.sql("SELECT * FROM ww_chnl_perf_app.test")
    validateDf.show(false)
    assert(validateDf.count() > 0)
    assert(
      validateDf
        .select("delta_flag")
        .distinct()
        .count()
        .equals(4L)
    )
    assert(
      validateDf
        .select("delta_flag")
        .distinct()
        .take(4)
        .map(_.getAs[String]("delta_flag"))
        .sorted
        .sameElements(Array("D", "I", "NC", "U"))
    )
    assert(
      validateDf
        .filter("delta_flag IN ('D', 'U')")
        .selectExpr("IF(upd_ts > load_ts, true, false) AS upd_ts_gt_load_ts")
        .filter("upd_ts_gt_load_ts = false")
        .distinct().count().equals(0L)
    )
  }

  test("mutateAuditColumns") {
    val df = spark.sql("SELECT 1 AS id")
    val dfWithAuditColumns = df.mutateAuditColumns
    dfWithAuditColumns.show()
    assert(dfWithAuditColumns.count() == 1)
    assert(dfWithAuditColumns.columns.sameElements(Array("id") ++ AUDIT_COLUMN_NAMES))
  }

  ignore("GCS location backup") {
    val df = spark.sql("SELECT *FROM ww_chnl_perf_app.test")
    df.backup("gs://bucket/ww_chnl_perf_stg.db/test_backup_user_given_date/")
    //OR
    //df.backup(getTableLocation("ww_chnl_perf_stg.test_backup_2022_01-08"))
  }
}
