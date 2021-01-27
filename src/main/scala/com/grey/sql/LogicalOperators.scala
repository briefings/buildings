package com.grey.sql

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

/**
  *
  * @param spark: An instance of SparkSession
  */
class LogicalOperators(spark: SparkSession) {

  /**
    * Operators: LIKE, IN, BETWEEN, IS NULL, AND, OR, NOT
    */
  def logicalOperators(): Unit = {

    println("\n\nLogical Operators")

    // logging
    val logger = Logger(classOf[LogicalOperators])
    logger.info("\n\nLogical Operators")

    // like: like is case sensitive
    spark.sql("SELECT * FROM buildings WHERE month_name LIKE 'Ju%'").show(3)
    spark.sql("SELECT * FROM buildings WHERE lower(month_name) LIKE 'ju%'").show(3)

    // in
    spark.sql("SELECT * FROM buildings WHERE " +
      "lower(month_name) IN ('march', 'june', 'september', 'december')").show(3)

    // between: SELECT * FROM buildings WHERE month BETWEEN 1 AND 3
    spark.sql("SELECT * FROM buildings WHERE month >= 1 AND month <= 3").show(3)

    // is null
    spark.sql("SELECT * FROM buildings WHERE month_name IS NULL").show(3)

    // and, or, not


  }

}
