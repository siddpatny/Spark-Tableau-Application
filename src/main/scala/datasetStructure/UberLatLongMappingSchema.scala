package datasetStructure

import org.apache.spark.sql.types.{DateType, DecimalType, FloatType, IntegerType, StringType, StructField, StructType}

object UberLatLongMappingSchema {
  val objectId = "OBJECTID"
  val joinCount = "Join_Count"
  val targetFid = "TARGET_FID"
  val dateTime = "Date_Time"
  val lat = "Lat"
  val long = "Lon"
  val base = "Base"
  val baseX = "Base_X"
  val baseY = "Base_Y"
  val zipcode = "ZIPCODE"
  val bldgZip = "BLDGZIP"
  val po_name = "PO_NAME"
  val population = "POPULATION"
  val area = "AREA"
  val state = "STATE"
  val county = "COUNTY"
  val st_fips = "ST_FIPS"
  val cty_fips = "CTY_FIPS"
  val URL = "URL"

  val customSchema = StructType(Array(
    StructField(objectId, StringType, true),
    StructField(joinCount, StringType, true),
    StructField(targetFid, StringType, true),
    StructField(dateTime, DateType, true),
    StructField(lat, StringType, true),
    StructField(long, StringType, true),
    StructField(base, StringType, true),
    StructField(baseX, StringType, true),
    StructField(baseY, StringType, true),
    StructField(zipcode, StringType, true),
    StructField(bldgZip, StringType, true),
    StructField(po_name, StringType, true),
    StructField(population, StringType, true),
    StructField(area, StringType, true),
    StructField(state, StringType, true),
    StructField(county, StringType, true),
    StructField(st_fips, StringType, true),
    StructField(cty_fips, StringType, true),
    StructField(URL, StringType, true))
  )

}
