package datasetManagers

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import datasetStructure.{Business, Event}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._


object EBDatasetManager {


   val events = sc.textFile("hdfs:///user/sp5331/big_data/NYC_Permitted_Event_Information.csv")
   val business = sc.textFile("hdfs:///user/sp5331/big_data/Legally_Operating_Businesses.csv")

   val business_cols = business.first()
   val business_rows = business.filter(row => row != business_cols)

   def stringtoTuple(s: String) =  {
    val arr = s.replaceAll("[\\(\\)]", "").split(",").map(_.trim)
    if (arr.length == 2)
      (arr(0).toDouble, arr(1).toDouble)
    else
      None
   }

   business_rows.take(2).foreach(println)

   def getBusiness(lines: RDD[String]): RDD[Business] =
    lines.map(line =>
      {
        val arr = line.split(",").map(_.trim)
        Business(b_type = arr(1),
          status = arr(3),
          start_date = LocalDate.parse(arr(4), DateTimeFormatter.ofPattern("MM/dd/yyyy")),
          end_date = LocalDate.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy")),
          address = if (arr(8) == "" && arr(9) == "") None else Some(arr(8) + "," + arr(9)),
          city = if (arr(12) == "") None else Some(arr(12)),
          state = if (arr(13) == "") None else Some(arr(13)),
          zip = if (arr(14) == "") None else Some(arr(14)),
          // location = if(arr(arr.length - 1) == "") None else Some((0.0,0.0))//Some(stringtoTuple(arr(26)))
          )
      })

   // val business_data = getBusiness(business_rows).filter(row=>row.b_type == "Business")
   // business_data.take(5).foreach(println)
   // business_data.persist()


   val events_cols = events.first()
   val events_rows = events.filter(row => row != events_cols) //.map(line=>line.split(",").map(_.trim))

   def getEvents(lines: RDD[String]): RDD[Event] =
   	lines.map(line =>
    	{
      	val arr = line.split(",").map(_.trim)
      	Event(id = arr(0).toInt,
      		e_type = arr(5),
      		start_date = if (arr(2)(2) == '/') LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")) else LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm")),
      		end_date = if (arr(3)(2) == '/') LocalDateTime.parse(arr(3), DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")) else LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm")),
      		address = if (arr(7) == "") None else Some(arr(7)),
      		borough = if (arr(6) == "") None else Some(arr(6))
        )
    	})

   val events_data = getEvents(events_rows)
   events_data.take(5).foreach(println)
   events_data.persist()
}