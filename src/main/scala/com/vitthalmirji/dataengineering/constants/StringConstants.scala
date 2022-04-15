package com.vitthalmirji.dataengineering.constants

object StringConstants {
  val COMMA                             = ","
  val FORWARD_SLASH                     = "/"
  val HYPHEN                            = "-"
  val UNDERSCORE                        = "_"
  val ERROR_CONSTANT                    = "ERROR"
  val STG                               = "stg"
  val SRC                               = "src"
  val HASHCODE                          = "hashcode"
  val FULL_OUTER                        = "full_outer"
  val INSERT_FLAG                       = "I"
  val UPDATE_FLAG                       = "U"
  val DELETE_FLAG                       = "D"
  val NO_CHANGE_FLAG                    = "NC"
  val USER_ID                           = "userid"
  val DELTA                             = "delta_flag"
  val UPD_TS                            = "upd_ts"
  val LOAD_TS                           = "load_ts"
  val COLUMNS_SEPARATOR_CHARACTER       = "~"
  val AUDIT_COLUMN_NAMES: Array[String] = Array(USER_ID, LOAD_TS, UPD_TS, DELTA)
}
