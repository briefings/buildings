package com.grey.inspectors

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import scala.util.Try
import scala.util.control.Exception

object InspectArguments {

  def inspectArguments(args: Array[String]): Parameters = {

    // Is the string args(0) a URL?
    val isURL: Try[Boolean] = new IsURL().isURL(args(0))

    // If the string is a valid URL parse & verify its parameters
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())

    val getParameters: Try[Parameters] = if (isURL.isSuccess){
      Exception.allCatch.withTry(
        mapper.readValue(new URL(args(0)), classOf[Parameters])
      )
    } else {
      sys.error(isURL.failed.get.getMessage)
    }

    if (getParameters.isSuccess) {
      getParameters.get
    } else {
      sys.error(getParameters.failed.get.getMessage)
    }

  }

  class Parameters(@JsonProperty("dataSource") _dataSource: String,
                   @JsonProperty("namesOf") _namesOf: String,
                   @JsonProperty("typeOf") _typeOf: String,
                   @JsonProperty("schemaOf") _schemaOf: String){

    require(_dataSource != null, "Parameter dataSource, i.e., data source URL, is required.")
    val dataSource: String = _dataSource

    require(_namesOf != null, "Parameter namesOf, i.e., the list of files URL, is required.")
    val namesOf: String = _namesOf

    require(_typeOf != null, "Parameter typeOf, i.e., the extension string of the files, is required.")
    val typeOf: String = _typeOf

    require(_schemaOf != null, "Parameter schemaOf, i.e., the data schema URL, is required.")
    val schemaOf: String = _schemaOf

  }

}
