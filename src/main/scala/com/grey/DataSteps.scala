package com.grey

import java.io.File
import java.nio.file.Paths

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import com.grey.metadata.ReadSchemaOf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try


class DataSteps(spark: SparkSession) {

  val localSettings = new LocalSettings()
  val regions: Map[String, String] = Map("1031" -> "northeast", "1046" -> "midwest", "1047" -> "south", "1034" -> "west")
  val codeLength: Int = 4

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {


    // Implicits
    import spark.implicits._

    // Schema of data
    val schemaOf: Try[StructType] = new ReadSchemaOf(spark).readSchemaOf(parameters)

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
    val pillar: DataFrame = sections.reduce(_ union _)

    // Pivot
    val readings = pillar.groupBy($"Period").pivot($"region").sum("Value")
    readings.show()
    println(readings.count())
    readings.printSchema()

  }

}
