package com.vitthalmirji.dataengineering.spark

import com.vitthalmirji.dataengineering.constants.StringConstants.{AUDIT_COLUMN_NAMES, COLUMNS_SEPARATOR_CHARACTER,
  DELETE_FLAG, DELTA, FORWARD_SLASH, FULL_OUTER, HASHCODE, INSERT_FLAG, LOAD_TS, NO_CHANGE_FLAG, SRC, STG,
  UPDATE_FLAG, UPD_TS, USER_ID}
import com.vitthalmirji.dataengineering.gcp.ObjectActions.parallelCopy
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.XxHash64
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ETL extends Serializable {

  @transient
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * [[com.vitthalmirji.dataengineering.spark.ETL#getAuditColumnsWithNewValues()]] returns Map of String &
   * SparkSQL Column with new values for all audit columns
   *
   * @return
   */
  def getAuditColumnsWithNewValues: Map[String, Column] =
    Map(
      USER_ID -> lit(SparkSession.getActiveSession.get.sparkContext.sparkUser),
      LOAD_TS -> current_timestamp,
      UPD_TS -> current_timestamp,
      DELTA -> lit(INSERT_FLAG)
    )

  /**
   * [[com.vitthalmirji.dataengineering.spark.ETL#prepareExecution(scala.collection.immutable.List)]]
   *
   * @param listOfSchemas List of schemas to be created in the DB before pipeline execution
   */

  def prepareExecution(listOfSchemas: List[String]): Unit =
    listOfSchemas.foreach { schema =>
      SparkSession.getActiveSession.get.sql(schema)
    }

  /**
   * @param ddl       DDL to be executed
   * @param schema    Schema to be used for DDL execution
   * @param tablename Tablename for DDL execution
   * @return
   */

  def getSchema(ddl: String, schema: String, tablename: String): String =
    ddl.replace("{SCHEMA}", schema).replace("{TABLENAME}", tablename)

  /**
   * Implicit class [[com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations]] provides set of functions to perform Change Data Capture (CDC) from traditional
   * Data warehousing practices viz. Slow Changing Dimension (SCD) Type 1, Type2 etc.
   * Provides chain functions on top DataFrame type viz. [[com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations#performDelta(scala.Option, scala.collection.immutable.List, scala.Option)]]
   * TODO SCD type 2 Delta CDC
   *
   * @param dataframe Dataset transformed / fetched from source fresh
   */
  implicit class DeltaTransformations(dataframe: DataFrame) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations#performDelta(scala.Option, scala.collection.immutable.List, scala.Option)]]
     * Function that performs Slow Changing Dimension Type 1 Delta on given datasets
     *
     * @param previousLoadedDataset             Dataset that was already loaded previously for comparision at row/value level
     *                                          This value will be None during initial load / history loads
     * @param primaryKeys                       List of Primary keys common between both datasets
     * @param columnsExcludeHashcodeCalculation List of Columns that must be skipped during hashcode computation
     * @return Dataset with column `delta_flag` having values `I`, `U`, `D`, `NC`
     *         Usage:
     *         For Initial / History load:
     *         val sourceQuery = "SELECT t1.*, current_timestamp AS load_ts, current_timestamp AS upd_ts FROM source.test_source t1"
     *         val extractDf = spark.sql(sourceQuery)
     *         val deltaDf = extractDf.performDeltaType1SCD(previousLoadedDataset = None, primaryKeys = List("id", "bus_dt"),
     *         columnsExcludeHashcodeCalculation = None)
     *         deltaDf.show(false)
     *         deltaDf
     *         .select(spark.dataframe("ww_chnl_perf_app.test").columns.map(col):_*)
     *         .write
     *         .mode("overwrite")
     *         .format("orc")
     *         .insertInto("ww_chnl_perf_app.test")
     *
     *         val validateDf = spark.sql("SELECT DISTINCT delta_flag FROM ww_chnl_perf_app.test")
     *         validateDf.show(false)
     *         assert(validateDf.count().equals(1L))
     *         assert(validateDf.take(1)(0).getAs[String]("delta_flag").equals("I"))
     *         For Change Data capture subsequent loads:
     *         val sourceQuery =
     *         """SELECT t1.id, t1.name, t1.op_cmpny_cd, t1.bus_dt,
     *         |CASE WHEN t1.name = 'Christian' THEN 31 ELSE t1.age END AS age,
     *         |current_timestamp AS load_ts, current_timestamp AS upd_ts FROM source.test_source t1
     *         |WHERE t1.name <> 'Bob'
     *         |UNION ALL
     *         |SELECT 10 AS id, 'Ryan' AS name, 'WMT-US' AS op_cmpny_cd, '2021-01-11' AS bus_dt,
     *         |29 AS age, current_timestamp AS load_ts, current_timestamp AS upd_ts
     *         |""".stripMargin
     *         val extractDf = spark.sql(sourceQuery)
     *
     *         val targetTable = spark.dataframe("ww_chnl_perf_app.test")
     *
     *         val partitionsToDrop = targetTable
     *         .selectExpr("CAST (bus_dt AS STRING) AS bus_dt")
     *         .distinct()
     *         .take(100)
     *         .map(_.getAs[String]("bus_dt"))
     *         .map(dt => Partition(Map("bus_dt" -> dt))).toList
     *
     *         partitionsToDrop.drop("ww_chnl_perf_stg.test_bkp")
     *         partitionsToDrop.delete("ww_chnl_perf_stg.test_bkp")
     *
     *         targetTable.write.mode("overwrite").format("orc").insertInto("ww_chnl_perf_stg.test_bkp")
     *         val previousLoadedDf = spark.dataframe("ww_chnl_perf_stg.test_bkp")
     *         assert(previousLoadedDf.count() > 0)
     *
     *         val deltaDf = extractDf.performDeltaType1SCD(previousLoadedDataset = Some(previousLoadedDf),
     *         primaryKeys = List("id", "bus_dt"), columnsExcludeHashcodeCalculation = None)
     *         deltaDf.show(false)
     *
     *         partitionsToDrop.drop("ww_chnl_perf_app.test")
     *         partitionsToDrop.delete("ww_chnl_perf_app.test")
     *
     *         deltaDf
     *         .select(spark.dataframe("ww_chnl_perf_app.test").columns.map(col):_*)
     *         .write
     *         .mode("overwrite")
     *         .format("orc")
     *         .insertInto("ww_chnl_perf_app.test")
     *
     *         val validateDf = spark.sql("SELECT * FROM ww_chnl_perf_app.test")
     *         validateDf.show(false)
     *         assert(validateDf.count() > 0)
     *         assert(validateDf
     *         .select("delta_flag")
     *         .distinct()
     *         .count()
     *         .equals(4L))
     *         assert(
     *         validateDf
     *         .select("delta_flag")
     *         .distinct()
     *         .take(4)
     *         .map(_.getAs[String]("delta_flag"))
     *         .sorted
     *         .sameElements(Array("D", "I", "NC", "U"))
     *         )
     */
    def performDelta(previousLoadedDataset: Option[DataFrame], primaryKeys: List[String], columnsExcludeHashcodeCalculation: Option[List[String]] = None): DataFrame = {
      val deltaDf = previousLoadedDataset.map { previousDataset =>
        if (primaryKeys.isEmpty) {
          val errorMessage = "Primary keys cannot be empty"
          logger.error(errorMessage)
          throw new IllegalArgumentException(errorMessage)
        }

        val newDatasetColumns = dataframe.columns
        logger.warn(s"Source columns = $newDatasetColumns")
        val previousLoadedDatasetColumns = previousDataset.columns.filterNot(_.equals(DELTA))
        logger.warn(s"Stage columns except delta_flag = $previousLoadedDatasetColumns")
        val hashColumnsList = newDatasetColumns
          .filterNot(c =>
            (primaryKeys ++ AUDIT_COLUMN_NAMES ++ columnsExcludeHashcodeCalculation
              .getOrElse(List())).contains(c)
          )
        var joinExpr: Column = lit(true)
        primaryKeys.foreach(k => joinExpr = (col(s"$SRC.$k") === col(s"$STG.$k")) && joinExpr)

        logger.info(s"Hashcode will be computed on columns ${hashColumnsList.mkString(",")}")

        if (primaryKeys.map(k => newDatasetColumns.contains(k) && previousLoadedDatasetColumns.contains(k)).exists(_.equals(false))) {
          val errorMessage = s"Primary keys given are missing in datasets $primaryKeys"
          logger.error(errorMessage)
          throw new IllegalArgumentException(errorMessage)
        }

        val sourceDf = dataframe
          .withColumn(HASHCODE, hash64(concat_ws(COLUMNS_SEPARATOR_CHARACTER, hashColumnsList.map(col): _*)))
        val stageDf = if (previousDataset.columns.contains(DELTA)) {
          previousDataset
            .filter(s"`$DELTA` <> 'D'")
            .drop(DELTA)
            .withColumn(HASHCODE, hash64(concat_ws(COLUMNS_SEPARATOR_CHARACTER, hashColumnsList.map(col): _*)))
        } else {
          previousDataset
            .withColumn(HASHCODE, hash64(concat_ws(COLUMNS_SEPARATOR_CHARACTER, hashColumnsList.map(col): _*)))
        }

        val deltaDf = sourceDf
          .as(SRC)
          .join(stageDf.as(STG), joinExpr, FULL_OUTER)
          .withColumn(DELTA, when(col(s"$STG.${primaryKeys.head}").isNull, INSERT_FLAG)
            .when(col(s"$SRC.${primaryKeys.head}").isNull, DELETE_FLAG)
            .when(col(s"$SRC.$HASHCODE") === col(s"$STG.$HASHCODE"), NO_CHANGE_FLAG)
            .otherwise(UPDATE_FLAG))
          .select((previousLoadedDatasetColumns ++ Array(DELTA)).map(resolveAuditColumns): _*)

        if (logger.isDebugEnabled) {
          deltaDf.printSchema()
        }
        deltaDf.selectExpr(newDatasetColumns: _*)
      }
      deltaDf.getOrElse(dataframe.withColumn(DELTA, lit(INSERT_FLAG)))
    }

    /**
     * [[com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations#hash64(scala.collection.Seq)]] function that computes Hash64 using spark's
     * [[org.apache.spark.sql.catalyst.expressions.XxHash64#XxHash64(scala.collection.Seq)]]
     * Private function, not available for end users
     * TODO Change hashing logic: Try FNV / Murmur / MD5
     *
     * @param cols list of columns on which Hash64 myst be computed
     * @return a Spark SQL Column containing Hash64 value
     *         Usage:
     *         val sourceDf = newDataset.withColumn(HASHCODE, hash64(concat_ws("~", hashColumnsList.map(col): _*)))
     */
    private def hash64(cols: Column*): Column = new Column(new XxHash64(cols.map(_.expr)))

    /**
     * [[com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations#resolveAuditColumns(java.lang.String)]] function resolves selecting column with aliases
     * after any SQL JOIN
     * Private function, should not be used by End users
     *
     * @param columnName name of the field / column to resolve
     * @return a Spark SQL Column containing resolved value from particular dataset
     *         Usage:
     *         val deltaDf = sourceDf.as(SRC).join(stageDf.as(STG), joinExpr, FULL_OUTER)
     *         .withColumn(DELTA_FLAG, when(col(s"$STG.${primaryKeys.head}").isNull, INSERT_FLAG)
     *         .when(col(s"$SRC.${primaryKeys.head}").isNull, DELETE_FLAG)
     *         .when(col(s"$SRC.$HASHCODE") === col(s"$STG.$HASHCODE"), NO_CHANGE_FLAG).otherwise(UPDATE_FLAG))
     *         .select(previousLoadedDatasetColumns.map(resolveAuditColumns) :_*)
     */
    private def resolveAuditColumns(columnName: String): Column = {
      logger.warn(columnName)
      val resolvedColumn = columnName match {
        case DELTA => col(DELTA)
        case UPD_TS =>
          when(col(DELTA).isin(INSERT_FLAG, UPDATE_FLAG, DELETE_FLAG), current_timestamp())
            .otherwise(col(s"$STG.$columnName"))
            .alias(columnName)
        case LOAD_TS =>
          when(col(DELTA).isin(INSERT_FLAG), current_timestamp())
            .otherwise(col(s"$STG.$columnName"))
            .alias(columnName)
        case _ =>
          when(col(DELTA).isin(INSERT_FLAG, UPDATE_FLAG, NO_CHANGE_FLAG), col(s"$SRC.$columnName"))
            .otherwise(col(s"$STG.$columnName"))
            .alias(columnName)
      }
      resolvedColumn
    }

    /**
     * [[com.vitthalmirji.dataengineering.spark.ETL.DeltaTransformations#mutateAuditColumns()]]
     * adds audit columns with new values
     *
     * @return Returns dataframe added with Audit columns having new values
     *         Usage:
     *         val df = spark.sql("< some query >")
     *         val dfWithAuditColumns = df.mutateAuditColumns
     */
    def mutateAuditColumns: DataFrame = {
      var _dataframe = dataframe
      getAuditColumnsWithNewValues.foreach(c => _dataframe = _dataframe.withColumn(c._1, c._2))
      _dataframe
    }
  }

  /**
   * [[com.vitthalmirji.dataengineering.spark.ETL.ETLDataframeActions]] Implicit class provides set of functions on top of Dataframe
   *
   * @param dataframe Dataframe queried from table / produced
   */
  implicit class ETLDataframeActions(dataframe: DataFrame) extends Serializable {

    /**
     * [[com.vitthalmirji.dataengineering.spark.ETL.ETLDataframeActions#backup(java.lang.String)]]
     * Copies all underlying files of external table to new/given table's location
     *
     * @param backupLocationRootPath Location to copy/backup data
     * @return Returns boolean if all underlying files of external table copied successful, also prints file-locations which did not succeed copy
     *         Usage:
     *         val df = spark.sql("SELECT *FROM ww_chnl_perf_app.test")
     *         df.backup("gs://bucket/ww_chnl_perf_app_qa.db/test_backup_user_given_date/")
     *         Note: DO NOT FORGET TO ADD FORWARD SLASH IF TARGET PATH IS DIRECTORY example dir2/
     */
    def backup(backupLocationRootPath: String): Boolean = {
      val spark: SparkSession = SparkSession.getActiveSession.get
      val FILE_LOCATION = "file_location"
      val targetLocation = spark.sparkContext.broadcast(backupLocationRootPath)
      import spark.implicits.newStringEncoder
      val fileLocations = dataframe
        .select(input_file_name().alias(FILE_LOCATION))
        .distinct()
        .map(_.getAs[String](FILE_LOCATION))
        .rdd
        .mapPartitions { partition =>
          partition.map { location =>
            val locationTokens = location.split(FORWARD_SLASH)
            val tablePath = locationTokens
              .slice(locationTokens.indexWhere(_.contains(".db")) + 2, locationTokens.length)
              .mkString(FORWARD_SLASH)
            if (targetLocation.value.endsWith(FORWARD_SLASH)) {
              (location, targetLocation.value + tablePath)
            } else {
              (location, targetLocation.value + FORWARD_SLASH + tablePath)
            }
          }
        }
      parallelCopy(fileLocations)
    }
  }
}

