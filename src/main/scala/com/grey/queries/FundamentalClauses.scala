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


    println("\n\nFundamental Clauses")


    // logging
    val logger = Logger(classOf[FundamentalClauses])
    logger.info("\n\nFundamental Clauses")

    // select
    spark.sql("SELECT year, month, west FROM buildings").show(5)

    // limit
    spark.sql("SELECT * FROM buildings LIMIT 5").show()

    // order by
    spark.sql("SELECT date, year, month, month_name, midwest FROM buildings " +
      "ORDER BY year").show(5)

  }

}
