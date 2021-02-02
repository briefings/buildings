package com.grey.sql

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark : An instance of SparkSession
  */
class LogicalOperators(spark: SparkSession) {

  /**
    * Operators: LIKE, IN, BETWEEN, IS NULL, AND, OR, NOT
    */
  def logicalOperators(buildings: Dataset[Row]): Unit = {

    println("\n\nLogical Operators")


    // logging
    val logger = Logger(classOf[LogicalOperators])
    logger.info("\n\nLogical Operators")


    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._


    // LIKE & case sensitive problems: like is case sensitive
    val sensitive: Long = spark.sql("SELECT count(*) AS n FROM buildings WHERE month_name LIKE 'Ju%'")
      .head().getAs[Long]("n")

    val sensitiveSet: Long = buildings.filter($"month_name".startsWith("Ju")).count()

    println(s"The # of records, since 1968, wherein the month string starts with 'Ju' exactly" +
      s"\n sql: $sensitive, dataset: $sensitiveSet")


    // LIKE & case insensitive problems
    val insensitive: Long = spark.sql("SELECT * FROM buildings WHERE lower(month_name) LIKE 'ju%'").count()

    val insensitiveSet: Long = buildings.filter(lower($"month_name").startsWith("ju")).count()

    println(s"The # of records, since 1968, wherein the month string starts with 'ju', regardless of case" +
      s"\n sql: $insensitive, dataset: $insensitiveSet")


    // IN
    val listOf: Seq[String] = Seq("march", "june", "september", "december")
    val listOfString: String = listOf.mkString("'", "','", "'")

    println(s"The # of records, since 1968, associated with the months 'march', 'june', 'september', & 'december'\n" +
      "sql: " +
      spark.sql(s"SELECT * FROM buildings WHERE lower(month_name) IN ($listOfString)").count() +
      ", dataset: " +
      buildings.filter(lower($"month_name").isInCollection(listOf)).count()
    )


    // BETWEEN
    println(s"\nThe # of records whereby the month value is between 1 & 3, i.e., is 1, 2, or 3\n" +
      "sql: " +
      spark.sql("SELECT * FROM buildings WHERE month >= 1 AND month <= 3").count() +
      ", dataset: " +
      buildings.filter($"month".between(lowerBound = 1, upperBound = 3)).count()
    )


    // IS NULL
    println("The number of cases whereby the month name is null\n" +
      "sql: " +
      spark.sql("SELECT * FROM buildings WHERE month_name IS NULL").count() +
      ", dataset: " +
      buildings.filter($"month_name".isNull).count()
    )


    // and, or, not

  }

}
