package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class FundamentalOperators(spark: SparkSession) {

  /**
    * Examples: select, as, limit, order by
    */
  def fundamentalOperators(buildings: Dataset[Row]): Unit = {

    println("\n\nFundamentals")

    // logging
    val logger = Logger(classOf[FundamentalOperators])
    logger.info("\n\nFundamentals")

    // select
    spark.sql("SELECT year, month, west FROM buildings").show(5)
    spark.sql("SELECT west AS West_Region FROM buildings").show(5)

    // limit
    spark.sql("SELECT * FROM buildings LIMIT 5").show()

    // order by
    spark.sql("SELECT date, year, month, month_name, midwest FROM buildings " +
      "ORDER BY year").show(5)

  }

}
