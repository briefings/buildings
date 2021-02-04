package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

/**
  *
  * @param spark: An instance of SparkSession
  */
class ArithmeticExpressions(spark: SparkSession) {

  /**
    * Miscellaneous examples
    */
  def arithmeticExpressions(): Unit = {

    println("\n\nArithmetic Expressions")

    // logging
    val logger = Logger(classOf[ArithmeticExpressions])
    logger.info("\n\nArithmetic Expressions")


    // tallies of all regions
    spark.sql("SELECT year, month, " +
      "midwest, northeast, south, west, midwest + northeast + south + west AS sums FROM buildings").show(5)

    // percentages
    spark.sql("SELECT year, month, " +
      "100 * midwest/(midwest + northeast + south + west) AS midwest, " +
      "100 * northeast/(midwest + northeast + south + west) AS northeast, " +
      "100 * south/(midwest + northeast + south + west) AS south, " +
      "100 * west/(midwest + northeast + south + west) As west FROM buildings WHERE year >= 2000").show(5)

    spark.sql("with base as (SELECT year, month, midwest, northeast, south, west, " +
      "midwest + northeast + south + west AS sums FROM buildings WHERE year >= 2000) " +
      "SELECT year, month, 100 * midwest/sums AS midwest, 100 * northeast/sums AS northeast, " +
      "100 * south/sums AS south, 100 * west/sums AS west FROM base").show(5)

  }

}
