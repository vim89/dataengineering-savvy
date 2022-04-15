package com.vitthalmirji.dataengineering.spark

/**
 * [[com.vitthalmirji.dataengineering.spark.Partition]] case class represents placeholder for dataframe's partition
 * @param partitions is variable holding partition field name & value in Map[String, Any] data structure
 * For example hdfs://users/user/test_db.db/test_table/business_date=2022-01-01/company_code=WMT-US/week_nbr=4
 * param partitions hold Map("business_date" -> "2022-01-01", "op_cmpny_cd" -> "WMT-US", "week_nbr" -> 4)
 */
final case class Partition(partitions: Map[String, Any])