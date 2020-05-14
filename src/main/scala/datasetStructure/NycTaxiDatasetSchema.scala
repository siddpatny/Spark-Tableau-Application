package datasetStructure

import org.apache.spark.sql.types._

object NycTaxiDatasetSchemaActual {

  val vendorID = ("VendorID", 0)
  val pickupDateTime = ("tpep_pickup_datetime", 1)
  val dropOffDateTime = ("tpep_dropoff_datetime", 2)
  val passenger_count = ("passenger_count", 3)
  val trip_distance = ("trip_distance", 4)
  val ratecodeID = ("RatecodeID", 5)
  val store_and_fwd_flag = ("store_and_fwd_flag", 6)
  val PULocationID = ("PULocationID", 7)
  val DOLocationID = ("DOLocationID", 8)
  val payment_type = ("payment_type", 9)
  val fare_amount = ("fare_amount", 10)
  val extra = ("extra", 11)
  val mta_tax = ("mta_tax", 12)
  val tip_amount = ("tip_amount", 13)
  val tolls_amount = ("tolls_amount", 14)
  val improvement_surcharge = ("improvement_surcharge", 15)
  val total_amount = ("total_amount", 16)

}

object NycTaxiDatasetSchemaCleaned {

  val vendorID = ("VendorID", 0, 1)
  val pickupDateTime = ("tpep_pickup_datetime", 1, 2)
  val dropOffDateTime = ("tpep_dropoff_datetime", 2, 3)
  val passenger_count = ("passenger_count", 3, 4)
  val PULocationID = ("PULocationID", 4, 5)
  val DOLocationID = ("DOLocationID", 5, 6)
  val taxi_count_airport = ("taxi_count_airport", 6, 7)
  val passenger_count_airport = ("passenger_count_airport", 6, 7)

  def getHeaders: Array[String] = {
    Array(vendorID._1, pickupDateTime._1, dropOffDateTime._1, passenger_count._1, PULocationID._1, DOLocationID._1)
  }

  val customSchema = StructType(Array(
    StructField(vendorID._1, IntegerType, true),
    StructField(pickupDateTime._1, DateType, true),
    StructField(dropOffDateTime._1, DateType, true),
    StructField(passenger_count._1, IntegerType, true),
    StructField(PULocationID._1, IntegerType, true),
    StructField(DOLocationID._1, IntegerType, true))
  )

}

object NycTaxiDatasetSchemaMapped {

  val vendorID = ("VendorID", 0, 1)
  val pickupDateTime = ("tpep_pickup_datetime", 1, 2)
  val dropOffDateTime = ("tpep_dropoff_datetime", 2, 3)
  val passenger_count = ("passenger_count", 3, 4)
  val PULocationID = ("PULocationID", 4, 5)
  val DOLocationID = ("DOLocationID", 5, 6)
  val PUZipCode = ("PUZipCode", 6, 7)
  val DOZipCode = ("DOZipCode", 7, 8)

  def getHeaders: Array[String] = {
    Array(vendorID._1, pickupDateTime._1, dropOffDateTime._1, passenger_count._1, PULocationID._1, DOLocationID._1,
      PUZipCode._1,DOZipCode._1)
  }

  val customSchema = StructType(Array(
    StructField(vendorID._1, IntegerType, true),
    StructField(pickupDateTime._1, DateType, true),
    StructField(dropOffDateTime._1, DateType, true),
    StructField(passenger_count._1, IntegerType, true),
    StructField(PULocationID._1, IntegerType, true),
    StructField(DOLocationID._1, IntegerType, true),
    StructField(PUZipCode._1, StringType, true),
    StructField(DOZipCode._1, StringType, true))
  )

}