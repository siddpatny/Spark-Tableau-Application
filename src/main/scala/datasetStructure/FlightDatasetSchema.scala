package datasetStructure

import datasetStructure.EventsDatasetSchema.{address, borough, e_type, end_date, id, map_zipcode, start_date, zipcode}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object FlightDatasetSchema {
  val YEAR = "YEAR"
  val MONTH = "MONTH"
  val DAY_OF_MONTH = "DAY_OF_MONTH"
  val flight_date = "FL_DATE"
  val OP_UNIQUE_CARRIER = "OP_UNIQUE_CARRIER"
  val ORIGIN_AIRPORT_ID = "ORIGIN_AIRPORT_ID"
  val ORIGIN_AIRPORT_SEQ_ID = "ORIGIN_AIRPORT_SEQ_ID"
  val ORIGIN_CITY_MARKET_ID = "ORIGIN_CITY_MARKET_ID"
  val ORIGIN = "ORIGIN"
  val ORIGIN_CITY_NAME = "ORIGIN_CITY_NAME"
  val DEST_AIRPORT_ID = "DEST_AIRPORT_ID"
  val DEST_AIRPORT_SEQ_ID = "DEST_AIRPORT_SEQ_ID"
  val DEST_CITY_MARKET_ID = "DEST_CITY_MARKET_IDT"
  val DEST_CITY_NAME = "DEST_CITY_NAME"
  val CRS_DEP_TIME = "CRS_DEP_TIME"
  val DEP_TIME = "DEP_TIME"
  val CRS_ARR_TIME = "CRS_ARR_TIME"
  val ARR_TIME = "ARR_TIME"
  val DEST = "DEST"
}

object FlightDatasetSchemaCleaned {
  val flight_date = "FL_DATE"
  val ORIGIN = "ORIGIN"
  val DEST = "DEST"

  val taxi_zone_dest = "taxi_zone_dest"
  val taxi_zone_orig = "taxi_zone_orig"

  def getHeaders: Array[String] = {
    Array(flight_date, ORIGIN, DEST)
  }

  val corrMatrixSchema = StructType(Array(
    StructField("EWR", DoubleType, true),
    StructField("JFK", DoubleType, true),
    StructField("LGA", DoubleType, true))
  )

}