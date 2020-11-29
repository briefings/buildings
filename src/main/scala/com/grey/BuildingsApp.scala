package com.grey

import com.grey.inspectors.InspectArguments
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BuildingsApp {

  def main(args: Array[String]): Unit = {


    // Arguments
    if (args.length == 0) {
      sys.error("The YAML of parameters is required.")
    }


    // Inspect
    val inspectArguments = InspectArguments
    val parameters: InspectArguments.Parameters = inspectArguments.inspectArguments(args)


    // Limiting log data streams
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    // Spark session instance
    val spark = SparkSession.builder()
      .appName("buildings")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // Spark logs level
    spark.sparkContext.setLogLevel("ERROR")


    // Proceed
    new DataSteps(spark).dataSteps(parameters = parameters)


  }

}
