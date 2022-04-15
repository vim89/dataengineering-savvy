package com.vitthalmirji.dataengineering.spark

import com.vitthalmirji.dataengineering.constants.DateTimeConstants.TZ_CST
import com.vitthalmirji.dataengineering.datetime.DateTimeHelpers.extractDate
import com.vitthalmirji.dataengineering.gcp.GcsInterpolators.GcsInterpolator
import com.vitthalmirji.dataengineering.gcp.ObjectActions.BlobOperations
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.time.{LocalDate, LocalDateTime}
import scala.util.{Failure, Success, Try}

/**
 * Singleton Object [[com.vitthalmirji.dataengineering.spark.Table]]
 * represents library functions used on Spark / Hive tables
 */
object Table {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table#getTableLocation(java.lang.String)]]
   * function fetches Hive / Spark dataframe location; used mostly on external tables;
   * works for managed dataframe as well.
   *
   * @param tableName name of dataframe to fetch location
   * @return Location of the dataframe as String
   *         Usage - {{{
   * val tableLocation: String = getTableLocation(tableName = "test_db.office")
   * }}}
   */
  def getTableLocation(tableName: String): String = {
    assert(SparkSession.getActiveSession.isDefined)
    Try {
      SparkSession.getActiveSession.get
        .sql(s"DESC FORMATTED $tableName")
        .filter("col_name = 'Location'")
        .select("data_type")
        .take(1)(0)
        .getAs[String]("data_type")
    } match {
      case Success(value) => value
      case Failure(exception) =>
        val errorMessage =
          s"Error fetching dataframe location for dataframe $tableName: ${exception.getMessage}"
        logger.error(errorMessage)
        exception.printStackTrace()
        throw new Throwable(errorMessage)
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table#repairRefreshTable(java.lang.String)]]
   * function performs MSCK REPAIR TABLE and spark catalog refresh on dataframe
   *
   * @param tableName name of dataframe to perform spark catalog refresh
   */
  def repairRefreshTable(tableName: String): Unit = {
    assert(SparkSession.getActiveSession.isDefined)
    val spark = SparkSession.getActiveSession.get
    Try {
      spark.sql(s"MSCK REPAIR TABLE $tableName")
      spark.catalog.refreshTable(tableName)
    } match {
      case Success(_) => logger.warn(s"Refresh table $tableName successful")
      case Failure(exception) =>
        val errorMessage = s"Error refreshing table $tableName : ${exception.getMessage}"
        logger.error(errorMessage)
        exception.printStackTrace()
      // throw new Throwable(errorMessage)
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table#inferTypeGetSqlEquivalentString(java.lang.Object)]]
   * function returns Sql equivalent String quoted or unquoted
   *
   * @param value as generic type
   * @tparam T generic type specified / identified at runtime
   * @return returns quoted or unquoted string to support SQL syntax
   */
  def inferTypeGetSqlEquivalentString[T](value: T): String =
    value match {
      case s: String => s"'$s'"
      case d: DateTime => s"'${d.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))}'"
      case _ => value.toString
    }

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table#deleteDfsLocation(java.lang.String)]]
   * function physically deletes any file system location
   *
   * @param location path given as string
   */
  def deleteDfsLocation(location: String): Unit = {
    val tryDeleteDfs = Try {
      val path = getHadoopFsPath(location)
      val fileSystem = getFileSystem(path)
      logger.warn("Attempting to Delete location: " + path.toString)
      if (fileSystem.exists(path)) {
        fileSystem.delete(path, true)
      } else {
        logger.warn("Path " + path.toString + " doesn't exist, skipping")
      }
    }
    tryDeleteDfs match {
      case Success(_) => logger.warn(s"Delete location successful - $location")
      case Failure(exception) =>
        val errorMessage = s"Error deleting DFS location $location Cause = ${exception.getMessage}"
        logger.error(errorMessage)
        exception.printStackTrace()
        throw new Throwable(errorMessage)
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table#getHadoopFsPath(java.lang.String)]]
   * functions returns HadoopFS equivalent Path type of path given in String
   *
   * @param path path given in String
   * @return returns Hadoop FS equivalent Path type of path given in String
   */
  private def getHadoopFsPath(path: String): Path = new Path(path)

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table#getFileSystem(org.apache.hadoop.fs.Path)]]
   * function returns FileSystem
   *
   * @param path path given in HadoopFS Path
   * @return returns FileSystem (HDFS / GCS / S3)
   */
  private def getFileSystem(path: Path): FileSystem = {
    assert(SparkSession.getActiveSession.isDefined)
    path.getFileSystem(SparkSession.getActiveSession.get.sparkContext.hadoopConfiguration)
  }

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions]] is an
   * implicit class definition that provides 2 important functions
   * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions#getPartitions(boolean)]],
   * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions#drop(java.lang.String)]],
   * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions#delete(java.lang.String)]]
   * to operate specially on Spark/Hive dataframe's Partition placeholders
   * This uses com.walmart.luminate.cperf.spark.Partition as Partition placeholders
   *
   * @param tablePartitions list of derived / defined com.walmart.luminate.cperf.spark.Partition
   */
  implicit class ImplicitTablePartitionActions(tablePartitions: List[Partition]) extends Serializable {
    /**
     * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions#drop(java.lang.String)]]
     * function drops all listed partitions for dataframe given
     *
     * @param tableName name of dataframe for which listed Partitions must be dropped
     *                  Usage - {{{
     * val op_cmpny_cd = "WMT-US"
     * spark.sql("DROP TABLE IF EXISTS test_db.test_table_drop_partitions PURGE")
     * spark.sql("""
     *|CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table_drop_partitions (
     *|id INT) PARTITIONED BY(bus_dt STRING, op_cmpny_cd STRING, week_nbr INT) STORED AS ORC
     *|LOCATION 'spark-warehouse/test_db.db/test_table_drop_partitions'
     *|""".stripMargin)
     * val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq((1, "2021-01-01", op_cmpny_cd, 1),
     *(2, "2021-01-08", op_cmpny_cd, 2),
     *(3, "2021-01-16", "WMT.COM", 3),
     *(4, "2021-01-24", op_cmpny_cd, 4)))).toDF("id", "bus_dt", "op_cmpny_cd", "week_nbr")
     * df.write.mode("overwrite").insertInto("test_db.test_table_drop_partitions")
     *
     * val partitionsToDrop = List(
     *Partition(Map("bus_dt" -> "2021-01-01")),
     *Partition(Map("bus_dt" -> "2021-01-08")),
     *Partition(Map("bus_dt" -> "2021-01-24", "op_cmpny_cd" -> "WMT-US", "week_nbr"-> 4))
     *)
     *
     * partitionsToDrop.drop("test_db.test_table_drop_partitions")
     * }}}
     */
    def drop(tableName: String): Unit = {
      val partitionList = getPartitions(inferType = true)
      val partitionString =
        partitionList.map(partition => s"PARTITION(${partition.mkString(",")})").mkString(",")

      val query = s"ALTER TABLE $tableName DROP IF EXISTS $partitionString PURGE"
      logger.warn(s"Dropping partitions using query: $query")
      SparkSession.getActiveSession.foreach(_.sql(query).show())
    }

    /**
     * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions#getPartitions(boolean)]]
     * returns List containing Set of Strings after iterating through derived tablePartitions
     *
     * @param inferType If type should be inferred or not before returning as plain string or SQL equivalent string
     * @return returns plain string or SQL equivalent string quoted / unquoted
     */
    private def getPartitions(inferType: Boolean): List[Set[String]] =
      tablePartitions.map(partition =>
        partition.partitions.map { case (field, value) =>
          if (inferType) s"$field=${inferTypeGetSqlEquivalentString(value)}" else s"$field=${value.toString}"
        }.toSet
      )

    /**
     * [[com.vitthalmirji.dataengineering.spark.Table.ImplicitTablePartitionActions#delete(java.lang.String)]]
     * function physically deletes the location of dataframe's given list of partitions
     *
     * @param tableName name of dataframe for which listed Partitions must be physically deleted
     *                  Usage - {{{
     * val op_cmpny_cd = "WMT-US"
     * spark.sql("DROP TABLE IF EXISTS test_db.test_table_drop_partitions PURGE")
     * spark.sql("""
     *|CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table_drop_partitions (
     *|id INT) PARTITIONED BY(bus_dt STRING, op_cmpny_cd STRING, week_nbr INT) STORED AS ORC
     *|LOCATION 'spark-warehouse/test_db.db/test_table_drop_partitions'
     *|""".stripMargin)
     * val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq((1, "2021-01-01", op_cmpny_cd, 1),
     *(2, "2021-01-08", op_cmpny_cd, 2),
     *(3, "2021-01-16", "WMT.COM", 3),
     *(4, "2021-01-24", op_cmpny_cd, 4)))).toDF("id", "bus_dt", "op_cmpny_cd", "week_nbr")
     * df.write.mode("overwrite").insertInto("test_db.test_table_drop_partitions")
     *
     * val partitionsToDrop = List(
     *Partition(Map("bus_dt" -> "2021-01-01")),
     *Partition(Map("bus_dt" -> "2021-01-08")),
     *Partition(Map("bus_dt" -> "2021-01-24", "op_cmpny_cd" -> "WMT-US", "week_nbr"-> 4))
     *)
     *
     * partitionsToDrop.delete("test_db.test_table_drop_partitions")
     * }}}
     */
    def delete(tableName: String): Unit = {
      val partitionList = getPartitions(inferType = false)
      logger.warn(s"Attempting to delete partitions $partitionList")
      val tableLocation = getTableLocation(tableName)
      val dfsLocations =
        partitionList.map(partition => tableLocation + "/" + partition.mkString("/"))
      dfsLocations.foreach(deleteDfsLocation)
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.spark.Table.TableDataframeActions]] Implicit class provides action on Table wrapped in dataframe
   *
   * @param dataframe Table as whole or filtered using query wrapped into Dataframe
   */
  implicit class TableDataframeActions(dataframe: DataFrame) extends Serializable {
    /**
     * [[com.vitthalmirji.dataengineering.spark.Table.TableDataframeActions#getAffectedPartitionDates(java.time.LocalDateTime, java.lang.String)]]
     *
     * @param since     previous DateTime when this function was accessed or list all partitions that got impacted after (since) this DateTime
     * @param dateRegex By default yyyy-MM-dd; regex = \\d{4}-\\d{2}-\\d{2}, pass explicitly if other than default pattern
     * @return Returns list of date partitions of given dataframe that got affected after given since date
     *         Usage:
     *         val affectedDates = spark
     *         .sql("SELECT * FROM ww_chnl_perf_app.chnl_perf_item_fact_dly WHERE op_cmpny_cd = 'WMT-US'")
     *         .getAffectedPartitionDates(since = yearMonthDate24HrTs"2022-04-03 00:00:00")
     */
    def getAffectedPartitionDates(since: LocalDateTime, dateRegex: String = "\\d{4}-\\d{2}-\\d{2}"): Array[(LocalDate, LocalDateTime)] = {
      val FILE_LOCATION = "file_location"
      val affectedDates = dataframe.select(input_file_name().alias(FILE_LOCATION)).distinct().rdd.mapPartitions(partition =>
        partition.flatMap(field => {
          val location = field.getAs[String]("file_location")
          val blobLocation = blob"$location"
          val blobLastUpdated = blobLocation.updatedTs(Some(TZ_CST))
          if (blobLastUpdated.isAfter(since)) Some((extractDate(location, dateRegex), blobLastUpdated)) else None
        })
      ).reduceByKey((v1, v2) => if (v1.isAfter(v2)) v1 else v2).flatMap(t => t._1.map((_, t._2))).distinct().collect()
      affectedDates
    }
  }
}
