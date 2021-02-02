package com.grey.sql

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class RelationalOperators(spark: SparkSession) {

  /**
    * Operators: =, <> or !=, >, <, >=, <=
    *
    * @param buildings: For illustrating equivalent Dataset calculations
    */
  def relationalOperators(buildings: Dataset[Row]): Unit = {

    println("\n\nRelational Operators")

    // logging
    val logger = Logger(classOf[RelationalOperators])
    logger.info("\n\nRelational Operators")

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._


    // greater than
    val west: DataFrame = spark.sql("SELECT * FROM buildings WHERE west > 50")

    val westSet: Long = buildings.filter($"west" > 50).count()

    println(s"The # of months, since 1968, during which more than 50 thousand housing units " +
      s"where delivered in a month\n sql: ${west.count()}, dataset: $westSet")


    // less than
    val south: DataFrame = spark.sql("SELECT * FROM buildings WHERE south < 20")

    val southSet: Long = buildings.filter($"south" < 20).count()

    println(s"The # of months, since 1968, during which fewer than 20 thousand housing units " +
      s"where delivered in a month\n sql: ${south.count()}, dataset: $southSet")

    
    // equal to
    val february: DataFrame = spark.sql("SELECT * FROM buildings WHERE month_name = 'February'")

    val februarySet: Long = buildings.filter($"month_name" === "February").count()

    println(s"The # of records, i.e., the # of months, since 1968, associated with the month of February\n" +
      s"sql: ${february.count()}, dataset: $februarySet")

  }

}
