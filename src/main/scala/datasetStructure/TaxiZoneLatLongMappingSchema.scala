package datasetStructure

import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}

object TaxiZoneLatLongMappingSchema {
  val objectId_1 = "OBJECTID_1"
  val join_count = "Join_Count"
  val target_fid = "TARGET_FID"
  val objectId = "OBJECTID"
  val shape_len = "Shape_Leng"
  val zone = "zone"
  val locationId = "LocationID"
  val borough = "borough"
  val zipcode = "ZIPCODE"
  val bldgZip = "BLDGZIP"
  val po_name = "PO_NAME"
  val population = "POPULATION"
  val area = "AREA"
  val state = "STATE"
  val county = "COUNTY"
  val st_fips = "ST_FIPS"
  val cty_fips = "CTY_FIPS"
  val shapeLength = "Shape_Length"
  val shapeArea = "Shape_Area"

  val customSchema = StructType(Array(
    StructField(objectId_1, IntegerType, true),
    StructField(join_count, IntegerType, true),
    StructField(target_fid, IntegerType, true),
    StructField(objectId, IntegerType, true),
    StructField(shape_len, FloatType, true),
    StructField(zone, StringType, true),
    StructField(locationId, IntegerType, true),
    StructField(borough, StringType, true),
    StructField(zipcode, IntegerType, true),
    StructField(bldgZip, FloatType, true),
    StructField(po_name, StringType, true),
    StructField(population, FloatType, true),
    StructField(area, FloatType, true),
    StructField(state, StringType, true),
    StructField(county, StringType, true),
    StructField(st_fips, IntegerType, true),
    StructField(cty_fips, IntegerType, true),
    StructField(shapeLength, FloatType, true),
    StructField(shapeArea, FloatType, true))
  )


//  val objectId_1 = ("OBJECTID_1 ", 1)
//  val join_count = ("Join_Count ", 2)
//  val target_fid = ("TARGET_FID ", 3)
//  val objectId = ("OBJECTID ", 4)
//  val shape_len = ("Shape_Leng ", 5)
//  val zone = ("zone ", 6)
//  val locationId = ("LocationID ", 7)
//  val borough = ("borough ", 8)
//  val zipcode = ("ZIPCODE ", 9)
//  val bldgZip = ("BLDGZIP ", 10)
//  val po_name = ("PO_NAME ", 11)
//  val populatino = ("POPULATION ", 12)
//  val area = ("AREA ", 13)
//  val state = ("STATE ", 14)
//  val county = ("COUNTY ", 15)
//  val st_fips = ("ST_FIPS ", 16)
//  val CTY_Fips = ("CTY_FIPS ", 17)
//  val shapeLength = ("Shape_Length ", 18)
//  val shapeArea = ("Shape_Area ", 19)


}
