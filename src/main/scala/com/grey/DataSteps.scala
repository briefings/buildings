package com.grey

import com.grey.inspectors.InspectArguments
import com.grey.sources.{DataRead, DataReconfiguration}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel


class DataSteps(spark: SparkSession) {

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
    new com.grey.queries.FundamentalClauses(spark = spark).fundamentalClauses(buildings = buildingsSet)
    new com.grey.queries.FilteringOperators(spark = spark).filteringOperators(buildings = buildingsSet)
    new com.grey.queries.RelationalOperators(spark = spark).relationalOperators(buildings = buildingsSet)
    new com.grey.queries.LogicalOperators(spark = spark).logicalOperators(buildings = buildingsSet)

    new com.grey.queries.ArithmeticExpressions(spark = spark).arithmeticExpressions(buildings = buildingsSet)

  }

}
