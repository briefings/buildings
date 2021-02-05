package com.grey.sources

import org.apache.spark.sql.functions.{date_format, month, to_date, year}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class DataReconfiguration(spark: SparkSession) {

  def dataReconfiguration(frame: DataFrame): (DataFrame, Dataset[Row]) = {

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._
    
    // Initially
    println("\nThe original data structure")
    frame.show(5)

    // Pivot
    val pivoted: DataFrame = frame.groupBy($"Period").pivot($"region").sum("Value")
    println("The pivoted data structure")
    pivoted.show(5)

    // Extract the date, year, month, and month name from the date/period string
    val extendedFrame: DataFrame = pivoted.withColumn("date", to_date($"Period", "MMM-yyyy"))
      .withColumn("year", year($"date"))
      .withColumn("month", month($"date"))
      .withColumn("month_name", date_format($"date", "MMMM"))
      .drop($"Period")
    println("The enhanced data set")
    extendedFrame.show(5)
    
    // Possible?
    val caseClassOf = CaseClassOf.caseClassOf(schema = extendedFrame.schema)

    // Hence
    (extendedFrame, extendedFrame.as(caseClassOf))

  }

}
