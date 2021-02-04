package com.grey.queries

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * @param spark : An instance of SparkSession
  */
class LogicalOperators(spark: SparkSession) {

  /**
    * Operators: like, in, between, is null, and, or, not
    *
    * @param buildings: The Dataset[Row] of buildings
    */
  def logicalOperators(buildings: Dataset[Row]): Unit = {


    println("\n\nLogical Operators")


    // logging
    val logger = Logger(classOf[LogicalOperators])
    logger.info("\n\nLogical Operators")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Example: like & case sensitive problems (like is case sensitive)
      */
    val sensitive: Long = spark.sql("SELECT count(*) AS n FROM buildings WHERE month_name LIKE 'Ju%'")
      .head().getAs[Long]("n")
    val sensitiveSet: Long = buildings.filter($"month_name".like("Ju%")).count()
    val sensitiveStartsWith: Long = buildings.filter($"month_name".startsWith("Ju")).count()

    println(s"The # of records, since 1968, wherein the month string starts with 'Ju' exactly" +
      s"\nsql: $sensitive, dataset: $sensitiveSet, $sensitiveStartsWith")


    /**
      * Example: like & case insensitive problems
      */
    val insensitive: Long = spark.sql("SELECT * FROM buildings WHERE lower(month_name) LIKE 'ju%'").count()
    val insensitiveSet: Long = buildings.filter(lower($"month_name").like("ju%")).count()
    val insensitiveStartsWith: Long = buildings.filter(lower($"month_name").startsWith("ju")).count()

    println(s"The # of records, since 1968, wherein the month string starts with 'ju', regardless of case" +
      s"\nsql: $insensitive, dataset: $insensitiveSet, $insensitiveStartsWith")


    /**
      * Example: in
      */
    val listOf: Seq[String] = Seq("march", "june", "september", "december")
    val listOfString: String = listOf.mkString("'", "','", "'")

    val isMember: Long = spark.sql(s"SELECT * " +
      s"FROM buildings WHERE lower(month_name) IN ($listOfString)").count()
    val isMemberSet: Long = buildings.filter(lower($"month_name")
      .isInCollection(listOf)).count()

    println(s"\nThe # of records, since 1968, associated with the months 'march', 'june', 'september', & 'december'\n" +
      s"sql: $isMember, dataset: $isMemberSet"
    )


    /**
      * Example: between
      */
    println(s"\nThe # of records whereby the month value is between 1 & 3, i.e., is 1, 2, or 3\n" +
      "sql: " +
      spark.sql("SELECT * FROM buildings WHERE month >= 1 AND month <= 3").count() +
      ", dataset: " +
      buildings.filter($"month".between(lowerBound = 1, upperBound = 3)).count()
    )


    /**
      * Example: is null
      */
    println("\nThe number of cases wherein the month name is null\n" +
      "sql: " +
      spark.sql("SELECT * FROM buildings WHERE month_name IS NULL").count() +
      ", dataset: " +
      buildings.filter($"month_name".isNull).count()
    )


    // and, or, not

  }

}
