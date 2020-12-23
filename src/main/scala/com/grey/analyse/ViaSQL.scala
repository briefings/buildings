package com.grey.analyse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class ViaSQL(spark: SparkSession) {

  def viaSQL(extended: DataFrame): Unit = {

    // Temporary table
    extended.createOrReplaceTempView("buildings")

    // Preliminaries
    spark.sql("select * from buildings limit 5").show()
    spark.sql("select * from buildings where month = 1").show()

  }

}
