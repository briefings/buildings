package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class FundamentalClauses(spark: SparkSession) {

  /**
    * Focus: limit, order by
    *
    * @param buildings: The Dataset[Row] of buildings
    */
  def fundamentalClauses(buildings: Dataset[Row]): Unit = {


    println("\n\nFundamental Clauses\n")


    // logging
    val logger = Logger(classOf[FundamentalClauses])
    logger.info("\n\nFundamental Clauses")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Example: limit()
      */
    val limit: Int = 5

    println("\n\nA preview of fields year, month, and west w.r.t. sql & dataset, respectively")
    spark.sql(s"SELECT year, month, west FROM buildings LIMIT $limit").show()
    buildings.select($"year", $"month", $"west").show(limit)

    println("\n\nA preview of all fields w.r.t. sql & dataset, respectively")
    spark.sql(s"SELECT * FROM buildings LIMIT $limit").show()
    buildings.show(limit)


    /**
      * Example: order by
      */
    println("\n\nA preview of the fields year, date, month_name, and midwest wherein the " +
      "data is ordered by ascending midwest value, and ascending date; nulls last in each case.  " +
      "The previews are w.r.t. sql & dataset, respectively")

    spark.sql("SELECT year, date, month_name, midwest FROM buildings " +
      "ORDER BY midwest ASC NULLS LAST, date ASC NULLS LAST").show(5)

    buildings.select($"year", $"date", $"month_name", $"midwest")
      .orderBy($"midwest".asc_nulls_last, $"date".asc_nulls_last)

  }

}
