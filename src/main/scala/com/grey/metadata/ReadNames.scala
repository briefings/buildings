package com.grey.metadata

import java.io.File
import java.net.URL
import java.nio.file.Paths

import com.grey.directories.{DataDirectories, LocalSettings}
import com.grey.inspectors.InspectArguments
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Try
import scala.util.control.Exception

class ReadNames(spark: SparkSession) {

  private val dataDirectories = new DataDirectories()
  private val localSettings = new LocalSettings()

  def readNames(parameters: InspectArguments.Parameters): DataFrame = {


    // Preliminaries
    val directoryString = localSettings.resourcesDirectory + "names"
    val fileString: String = parameters.namesOf.split("/").reverse.head
    val directoryAndFileString = Paths.get(directoryString, fileString).toString


    // If the names directory does not exist create it; at present, the
    // delete & re-create approach doesn't suffice
    val make: Try[Boolean] = dataDirectories.make(new File(directoryString))


    // Unload the file of names ...
    val namesOf: Try[Unit] = if (make.isSuccess) { Exception.allCatch.withTry(
      FileUtils.copyURLToFile(new URL(parameters.namesOf),
        new File(directoryAndFileString))
    )} else {
      sys.error(make.failed.get.getMessage)
    }


    // Failure?
    if (namesOf.isFailure) {
      sys.error(namesOf.failed.get.getMessage)
    }


    // Explore
    val instance: StructField = schema.distinct.head
    println(instance)


    // Read
    spark.read.schema(schema).option("header", "true")
      .csv(directoryAndFileString)


  }


  // Schema of list ...
  val schema: StructType = new StructType()
    .add("name", StringType, nullable = false)
    .add("description", StringType, nullable = false)
    .add("code", IntegerType, nullable = false)


}
