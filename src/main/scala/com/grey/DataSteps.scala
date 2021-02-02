package com.grey

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import com.grey.sources.{DataRead, DataReconfiguration}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel


class DataSteps(spark: SparkSession) {

  val localSettings = new LocalSettings()
  val regions: Map[String, String] = Map("1031" -> "northeast", "1046" -> "midwest", "1047" -> "south", "1034" -> "west")
  val codeLength: Int = 4

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    // The data; original form
    val frame: DataFrame = new DataRead(spark = spark).dataRead(parameters = parameters)

    // The data; reconfigured
    val (buildingsFrame: DataFrame, buildingsSet: Dataset[Row]) = new DataReconfiguration(spark = spark)
      .dataReconfiguration(frame = frame)

    // persisting
    buildingsFrame.persist(StorageLevel.MEMORY_ONLY)
    buildingsSet.persist(StorageLevel.MEMORY_ONLY)
    buildingsFrame.createOrReplaceTempView("buildings")

    // Hence
    spark.sql("SHOW TABLES")

    // Queries
    new com.grey.sql.RelationalOperators(spark = spark).relationalOperators(buildings = buildingsSet)
    new com.grey.sql.LogicalOperators(spark = spark).logicalOperators(buildings = buildingsSet)

  }

}
