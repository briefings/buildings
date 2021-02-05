package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

/**
  *
  * @param spark : An instance of SparkSession
  */
class ArithmeticExpressions(spark: SparkSession) {

  /**
    * Miscellaneous examples
    *
    * @param buildings : The Dataset[Row] of buildings
    */
  def arithmeticExpressions(buildings: Dataset[Row]): Unit = {


    println("\n\nArithmetic Expressions\n")


    // logging
    val logger = Logger(classOf[ArithmeticExpressions])
    logger.info("\n\nArithmetic Expressions")


    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    /**
      *
      * Example: Addition across columns
      *
      */
    println("The number of housing units (thousands) built per month, i.e., per record, across all " +
      "regions; sql & dataset previews, respectively.")

    /**
      * SQL
      */
    spark.sql("SELECT year, month, midwest, northeast, south, west, " +
      "midwest + northeast + south + west AS sums FROM buildings").show(5)

    /**
      * Dataset[Row]
      */
    buildings.select($"year", $"month", $"midwest", $"northeast", $"south", $"west",
      ($"midwest" + $"northeast" + $"south" + $"west").as("sum")).show(5)


    /**
      *
      * Example: Percentages
      *
      */
    println("Per month, i.e., per record, the percentage of housing units built within each region; the previews are " +
      "sql & dataset previews, respectively.  There are two dataset options.")

    /**
      * SQL
      */
    spark.sql("SELECT year, month, " +
      "100 * midwest/(midwest + northeast + south + west) AS midwest, " +
      "100 * northeast/(midwest + northeast + south + west) AS northeast, " +
      "100 * south/(midwest + northeast + south + west) AS south, " +
      "100 * west/(midwest + northeast + south + west) As west FROM buildings WHERE year >= 2000"
    ).show(5)


    /**
      * Dataset[Row]
      */

    // A percentage function  
    val percentage: (Column, Column) => Column = (x: Column, y: Column) => {
      lit(100) * x.divide(y)
    }

    // The data in focus
    val baseline: Dataset[Row] = buildings.filter($"year" > 2000).
      withColumn("sum", $"midwest" + $"northeast" + $"south" + $"west")

    // Hence, either
    baseline.select($"year", $"month",
      percentage($"midwest", $"sum").as("midwest"),
      percentage($"northeast", $"sum").as("northeast"),
      percentage($"south", $"sum").as("south"),
      percentage($"west", $"sum").as("west")
    ).show(5)

    // or
    var percentages: Dataset[Row] = baseline
    Seq("midwest", "northeast", "south", "west").foreach { field =>
      percentages = percentages.withColumn(field, percentage(percentages.col(field), percentages.col("sum")))
    }
    percentages.select($"year", $"month", $"midwest", $"northeast", $"south", $"west").show(5)

  }

}
