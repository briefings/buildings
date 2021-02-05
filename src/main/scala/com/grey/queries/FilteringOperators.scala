package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class FilteringOperators(spark: SparkSession) {

  /**
    * Focus: where, filter
    *
    * @param buildings: The Dataset[Row] of buildings
    */
  def filteringOperators(buildings: Dataset[Row]): Unit = {


    println("\n\nFilters\n")


    // logging
    val logger = Logger(classOf[FilteringOperators])
    logger.info("\n\nFilters")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Example: SQL -> where, Dataset[Row] -> where
      */
    println("Filtering via SQL -> where, Dataset[Row] -> where; the " +
      "previews are w.r.t. sql & dataset, respectively")

    spark.sql("SELECT year, date, month, northeast FROM buildings " +
      "WHERE month = 1 ORDER BY northeast ASC NULLS LAST").show(5)

    buildings.where($"month" === 1)
      .select($"year", $"date", $"month", $"northeast")
      .sort($"northeast".asc_nulls_last).show(5)


    /**
      * Example: SQL -> where, Dataset[Row] -> filter
      */
    println("Filtering via SQL -> where, Dataset[Row] -> filter; the " +
      "previews are w.r.t. sql & dataset, respectively")

    spark.sql("SELECT year, date, month_name, midwest FROM buildings " +
      "WHERE year > 2018 ORDER BY midwest DESC NULLS LAST").show(5)

    buildings.filter($"year" > 2018)
      .select($"year", $"date", $"month_name", $"midwest")
      .sort($"midwest".desc_nulls_last).show(5)

  }

}
