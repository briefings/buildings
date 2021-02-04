package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

/**
  *
  * @param spark: An instance of SparkSession
  */
class FilteringOperators(spark: SparkSession) {

  /**
    * Examples: where
    */
  def filteringOperators(): Unit = {

    println("\n\nFilters")

    // logging
    val logger = Logger(classOf[FilteringOperators])
    logger.info("\n\nFilters")

    // where
    spark.sql("SELECT * FROM buildings WHERE month = 1").show(5)

    spark.sql("SELECT date, year, month, month_name, midwest FROM buildings " +
      "WHERE year > 2018 ORDER BY year").show(5)

  }

}
