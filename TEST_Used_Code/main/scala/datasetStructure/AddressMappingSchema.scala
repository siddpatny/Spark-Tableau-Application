package datasetStructure


import org.apache.spark.sql.types.{StringType, StructField, StructType}


object AddressMappingSchema {

//  val end_date = "end_date"
//  val start_date = "start_date"
//  val e_type = "e_type"
//  val id = "formatted_address"

  def getHeaders: Array[String] = {
//    Array(borough,address,end_date,start_date,e_type,id,zipcode)
    Array(borough,address,zipcode)
  }
}
