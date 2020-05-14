package datasetStructure

import java.sql.Date

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

case class Business(b_type: String, status: String, start_date: java.time.LocalDate, end_date: java.time.LocalDate, address: Option[String], city: Option[String], state: Option[String], zipcode: Option[String]) extends Serializable

object BusinessDatasetSchema {
  case class BusinessDataframe(zipcode: Integer, state: String, city: String, address: String, end_date: Date, start_date: Date, status: String, b_type: String) extends Serializable

  val zipcode = "zipcode"
  val state = "state"
  val city = "city"
  val address = "address"
  val end_date = "end_date"
  val start_date = "start_date"
  val status = "status"
  val b_type = "b_type"

  val schema = ScalaReflection.schemaFor[BusinessDataframe].dataType.asInstanceOf[StructType]

  val customSchema = StructType(Array(
    StructField(zipcode, IntegerType, true),
    StructField(state, StringType, true),
    StructField(city, StringType, true),
    StructField(address, StringType, true),
    StructField(end_date, DateType, true),
    StructField(start_date, DateType, true),
    StructField(status, StringType, true),
    StructField(b_type, StringType, true))
  )

}
