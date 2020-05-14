package datasetStructure

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

//case class Event_No_Zipcode(id: Int, e_type: String, start_date: java.time.LocalDateTime, end_date: java.time.LocalDateTime, address: Option[String], borough: Option[String]) extends Serializable
case class Event(id: Int, e_type: String, start_date: java.time.LocalDateTime, end_date: java.time.LocalDateTime, address: Option[String], borough: Option[String], zipcode: String) extends Serializable

object EventsDatasetSchema {
//  val schema = ScalaReflection.schemaFor[Event].dataType.asInstanceOf[StructType]
//  val schema_nozip = ScalaReflection.schemaFor[Event_No_Zipcode].dataType.asInstanceOf[StructType]
val zipcode = "zipcode"
  val borough = "borough"
  val address = "address"
  val end_date = "end_date"
  val start_date = "start_date"
  val e_type = "e_type"
  val id = "id"
  val map_zipcode = "map_zipcode"

  val customSchema = StructType(Array(
    StructField(zipcode, StringType, true),
    StructField(borough, StringType, true),
    StructField(address, StringType, true),
    StructField(end_date, DateType, true),
    StructField(start_date, DateType, true),
    StructField(e_type, StringType, true),
    StructField(id, StringType, true))
  )

  val customSchema2 = StructType(Array(
    StructField(zipcode, StringType, true),
    StructField(borough, StringType, true),
    StructField(address, StringType, true),
    StructField(end_date, DateType, true),
    StructField(start_date, DateType, true),
    StructField(e_type, StringType, true),
    StructField(id, StringType, true),
    StructField(map_zipcode, StringType, true))
  )
}
