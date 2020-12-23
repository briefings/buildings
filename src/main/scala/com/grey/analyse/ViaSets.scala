package com.grey.analyse

import org.apache.spark.sql.{DataFrame, SparkSession}

class ViaSets(spark: SparkSession) {

  def viaSets(extended: DataFrame): Unit = {

    // Implicits
    //import spark.implicits._

    // Preliminaries
    extended.show(5)



  }

}
