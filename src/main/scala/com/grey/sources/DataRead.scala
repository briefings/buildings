package com.grey.sources

import java.io.File
import java.nio.file.Paths

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try

class DataRead(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val regions: Map[String, String] = Map("1031" -> "northeast", "1046" -> "midwest", "1047" -> "south", "1034" -> "west")
  private val codeLength: Int = 4

  def dataRead(parameters: InspectArguments.Parameters): DataFrame = {

    // Schema of data
    val schemaOf: Try[StructType] = new SchemaOf(spark).schemaOf(parameters)

    // List of data files
    val listOf = new ListOfFiles()
    val listOfFiles: List[File] = listOf.listOfFiles(
      dataDirectory = Paths.get(localSettings.resourcesDirectory, "data").toString,
      listOfExtensions = List(".csv"),
      listOfPatterns = List("Series")
    )

    // Sections
    val sections: ParSeq[DataFrame] = listOfFiles.par.map { file =>

      val code = file.toString
        .takeRight(codeLength + parameters.typeOf.length)
        .dropRight(parameters.typeOf.length)

      val data = spark.read.schema(schemaOf.get)
        .format("csv")
        .option("header", value = true)
        .load(file.toString)

      data.withColumn("region", lit(regions(code)))

    }

    // Reduce
    sections.reduce(_ union _)

  }

}
