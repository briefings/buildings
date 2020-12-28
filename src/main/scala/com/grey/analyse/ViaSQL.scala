package com.grey.analyse

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class ViaSQL(spark: SparkSession) {

  def viaSQL(extended: DataFrame): Unit = {


    // logging
    val logger = Logger(classOf[ViaSQL])


    // persisting
    extended.persist(StorageLevel.MEMORY_ONLY)


    // Temporary table
    extended.createOrReplaceTempView("buildings")


    // select
    println("\n\nselect")
    spark.sql("SELECT year, month, month_name, west, midwest, south, northeast FROM buildings").show(3)
    spark.sql("SELECT year, month, west FROM buildings").show(3)
    spark.sql("SELECT west AS West_Region FROM buildings").show(3)


    // limit
    println("\n\nlimit")
    spark.sql("SELECT * FROM buildings LIMIT 5").show()


    // where
    println("\n\nwhere")
    spark.sql("SELECT * FROM buildings WHERE month = 1").show(3)


    // comparison operators
    println("\n\ncomparisons")
    val west: DataFrame = spark.sql("SELECT * FROM buildings WHERE west > 50")
    logger.info(west.count().toString)
    println("west > 50 thousand units: " + west.count())

    val south: DataFrame = spark.sql("SELECT * FROM buildings WHERE south < 20")
    logger.info(south.count().toString)
    println("south <= 20 thousand units: " + south.count() + "\n\n")

    spark.sql("SELECT * FROM buildings WHERE month_name = 'February'").show(3)
    spark.sql("SELECT count(*) AS n FROM buildings WHERE month_name = 'February'").show()


    // Arithmetic
    println("\n\nArithmetic")
    spark.sql("SELECT year, month, south, west, south + west AS southwest FROM buildings").show(3)
    spark.sql("SELECT year, month, " +
      "midwest, northeast, south, west, midwest + northeast + south + west AS sums FROM buildings").show(3)
    spark.sql("SELECT year, month, midwest, northeast, west " +
      "FROM buildings WHERE west > midwest + northeast").show(3)
    spark.sql("SELECT year, month, " +
      "100 * midwest/(midwest + northeast + south + west) AS midwest, " +
      "100 * northeast/(midwest + northeast + south + west) AS northeast, " +
      "100 * south/(midwest + northeast + south + west) AS south, " +
      "100 * west/(midwest + northeast + south + west) As west FROM buildings WHERE year >= 2000").show(3)
    spark.sql("with base as (SELECT year, month, midwest, northeast, south, west, " +
      "midwest + northeast + south + west AS sums FROM buildings WHERE year >= 2000) " +
      "SELECT year, month, 100 * midwest/sums AS midwest, 100 * northeast/sums AS northeast, " +
      "100 * south/sums AS south, 100 * west/sums AS west FROM base").show(3)


    // like: like is case sensitive
    // Option: https://dataschool.com/how-to-teach-people-sql/how-regex-works-in-sql/
    println("\n\nlike")
    spark.sql("SELECT * FROM buildings WHERE month_name LIKE 'Ju%'").show(3)
    spark.sql("SELECT * FROM buildings WHERE lower(month_name) LIKE 'ju%'").show(3)


    // in
    println("\n\nin")
    spark.sql("SELECT * FROM buildings WHERE " +
      "lower(month_name) IN ('march', 'june', 'september', 'december')").show(9)


    // between: SELECT * FROM buildings WHERE month BETWEEN 1 AND 3
    println("\n\nbetween")
    spark.sql("SELECT * FROM buildings WHERE month >= 1 AND month <= 3").show(9)


    // is null
    println("\n\nis null")
    spark.sql("SELECT * FROM buildings WHERE month_name IS NULL").show(9)


    // and, or, not

    // order
    println("\n\norder")
    spark.sql("SELECT date, year, month, month_name, midwest FROM buildings " +
      "WHERE year > 2018 ORDER BY year").show(9)

  }

}
