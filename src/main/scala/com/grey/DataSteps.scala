package com.grey

import com.grey.inspectors.InspectArguments
import com.grey.metadata.{ReadNames, ReadSchema}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class DataSteps(spark: SparkSession) {

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    val namesOf: DataFrame = new ReadNames(spark).readNames(parameters)
    val schemaOf: Try[StructType] = new ReadSchema(spark).readSchema(parameters)

    namesOf.show()
    schemaOf.foreach(println(_))

  }

}
