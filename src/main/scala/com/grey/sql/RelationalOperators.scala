package com.grey.sql

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class RelationalOperators(spark: SparkSession) {

  /**
    * Operators: =, <> or !=, >, <, >=, <=
    */
  def relationalOperators(): Unit = {

    println("\n\nRelational Operators")

    // logging
    val logger = Logger(classOf[RelationalOperators])
    logger.info("\n\nRelational Operators")

    // greater than
    val west: DataFrame = spark.sql("SELECT * FROM buildings WHERE west > 50")
    println("west > 50 thousand units: " + west.count())

    // less than
    val south: DataFrame = spark.sql("SELECT * FROM buildings WHERE south < 20")
    println("south < 20 thousand units: " + south.count())

    // equal to
    val february = spark.sql("SELECT * FROM buildings WHERE month_name = 'February'")
    println("february: " + february.count())
    february.show(5)

  }

}
