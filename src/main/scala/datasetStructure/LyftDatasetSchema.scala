package datasetStructure

import org.apache.spark.sql.types.{DateType, FloatType, StructField, StructType}

object LyftDatasetSchemaActual {
  val datetime = ("time_of_trip", 0, 1)
  val lat = ("start_lat", 1, 2)
  val long = ("start_lng", 2, 3)
}

object LyftDatasetSchemaCleaned {
  val dateTime = ("start_date", 0, 1)
  val lat = ("Lat", 1, 2)
  val long = ("Lon", 2, 3)
  val zipcode = ("zipcode",3,4)

  def getHeaders: Array[String] = {
    Array(dateTime._1, lat._1, long._1)
  }

  val customSchema = StructType(Array(
    StructField(dateTime._1, DateType, true),
    StructField(lat._1, FloatType, true),
    StructField(long._1, FloatType, true))
  )
  val customSchema2 = StructType(Array(
    StructField(dateTime._1, DateType, true),
    StructField(zipcode._1, FloatType, true),
    StructField(lat._1, FloatType, true),
    StructField(long._1, FloatType, true))
  )
}