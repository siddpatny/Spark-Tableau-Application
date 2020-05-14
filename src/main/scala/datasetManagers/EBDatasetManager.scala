package datasetManagers

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import datasetStructure.{Business, Event}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._


object EBDatasetManager
{

  def getEvents(lines: RDD[String]): RDD[Event] =
    lines.map(line =>
    {
      try{
        val arr = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim)

        //      println(arr.length)
        Event(id = arr(0).toInt,
          e_type = arr(5),
          start_date = if (arr(2)(2) == '/') LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")) else LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm")),
          end_date = if (arr(3)(2) == '/') LocalDateTime.parse(arr(3), DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")) else LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm")),
          address = {
            if (arr(7) == "") None
            else if (arr(7)(0) == '\"')  Some(arr(7).substring(1,arr(7).length - 1).split(",")(0).replaceAll("[^0-9a-zA-Z:' ]"," ").replaceAll(" +"," "))
            else Some(arr(7).split(",")(0).replaceAll("[^0-9a-zA-Z:' ]"," ").replaceAll(" +"," "))
          },
          borough = if (arr(6) == "") None else Some(arr(6)),
          zipcode = ""
        )}
      catch {
        case _: Exception =>  null
      }
    })

  def getBusiness(lines: RDD[String]): RDD[Business] =
    lines.map(line =>
    {
      try{
        val arr = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)").map(_.trim)
        //       println(arr(10) + "::" + arr(11) + "::" + arr(12) + "::" +arr(13))
        Business(b_type = arr(1),
          status = arr(3),
          start_date =  if (arr(4)(2) == '/') LocalDate.parse(arr(4), DateTimeFormatter.ofPattern("MM/dd/yyyy")) else LocalDate.parse(arr(4), DateTimeFormatter.ofPattern("MM-dd-yyyy")),
          end_date = if (arr(2)(2) == '/') LocalDate.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy")) else LocalDate.parse(arr(2), DateTimeFormatter.ofPattern("MM-dd-yyyy")),
          address = if (arr(8) == "" && arr(9) == "") None else Some(arr(8) + " " + arr(9)),
          city = if (arr(11) == "") None else Some(arr(11)),
          state = if (arr(12) == "") None else Some(arr(12)),
          zipcode = if (arr(13) == "") None else Some(arr(13))
          // location = if(arr(arr.length - 1) == "") None else Some((0.0,0.0))//Some(stringtoTuple(arr(26)))
        )}
      catch {
        case _: Exception =>  null
      }
    })

  def classAccessors[T: TypeTag]: String = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.name
  }.mkString(",")


  def cleanEvents(eventDatasetFileName: String, spark: SparkSession, sc: SparkContext):  (Array[String], RDD[Event]) = {

    val events = sc.textFile(eventDatasetFileName)
    val header = events.first()

    val events_rows = events.filter(row => row != header).distinct()
    //    println(header)
    println(events_rows.count())
    val events_data = getEvents(events_rows).filter(line => (line != null && line.address != None && line.id != None))
    val events_cols = classAccessors[Event].split(",")
    println(events_cols.mkString(","))
    println(events_data.count())
    (events_cols,events_data)
  }

  def cleanBusiness(businessDatasetFileName: String, spark: SparkSession, sc: SparkContext):  (Array[String], RDD[Business]) = {

    val business = sc.textFile(businessDatasetFileName)
    val header = business.first()

    val business_rows = business.filter(row => row != header)
    println(business_rows.count())
    val business_data = getBusiness(business_rows).filter(line=> (line != null && line.zipcode != None && line.b_type != None)).filter(row=>row.b_type == "Business")
    val business_cols = classAccessors[Business].split(",")
    println(business_cols.mkString(","))
    println(business_data.count())
    (business_cols,business_data)

  }

  def profileEvents(eventDatasetFileName: String, spark: SparkSession, sc: SparkContext): List[String] = {

    val dataset = sc.textFile(eventDatasetFileName)
    val eventDataset = dataset.map(line=> line.split(",").map(_.trim))

    var profileList = List.empty[String]
    val csvHeader = eventDataset.first()

    profileList = profileList :+ "Number of Rows in Original Events = "+ (eventDataset.count() - 1).toString
    profileList :+= "Headers in Events Dataset = "+ csvHeader.mkString(", ")

    val cleanRdd = cleanEvents(eventDatasetFileName, spark, sc)

    profileList :+= "Number of Rows in Cleaned Events = "+ (cleanRdd._2.count() + 1)
    profileList :+= "Headers in cleaned Events Dataset = "+ cleanRdd._1.mkString(", ")

    val startMonthCount = cleanRdd._2.map(line => (line.start_date.getMonth, 1)).reduceByKey(_ + _).collect()
    startMonthCount.map(line => profileList :+= ("Number of Events in " + line._1 + " = " + line._2))

    val endMmonthCount = cleanRdd._2.map(line => (line.end_date.getMonth, 1)).reduceByKey(_ + _).collect()
    endMmonthCount.map(line => profileList :+= ("Number of Events in " + line._1 + " = " + line._2))

    profileList
  }

  def profileBusiness(businessDatasetFileName: String, spark: SparkSession, sc: SparkContext): List[String] = {
    val dataset = sc.textFile(businessDatasetFileName)
    val businessDataset = dataset.map(line=> line.split(",").map(_.trim))

    var profileList = List.empty[String]
    val csvHeader = businessDataset.first()

    profileList = profileList :+ "Number of Rows in Original Events = "+ (businessDataset.count() - 1).toString
    profileList :+= "Headers in Business Dataset = "+ csvHeader.mkString(", ")

    val cleanRdd = cleanBusiness(businessDatasetFileName, spark, sc)

    profileList :+= "Number of Rows in Cleaned Business = "+ (cleanRdd._2.count() + 1)
    profileList :+= "Headers in cleaned Business Dataset = "+ cleanRdd._1.mkString(", ")

    val bZipCount = cleanRdd._2.map(line => (line.zipcode, 1)).reduceByKey(_ + _).collect()
    bZipCount.map(line => profileList :+= ("Number of Businesses in " + line._1 + " = " + line._2))


    profileList
  }

}

// val events = sc.textFile("hdfs:///user/sp5331/big_data/NYC_Permitted_Event_Information.csv")
// val business = sc.textFile("hdfs:///user/sp5331/big_data/Legally_Operating_Businesses.csv")

// val business_cols = business.first()
// val business_rows = business.filter(row => row != business_cols)

// def stringtoTuple(s: String) =  {
//  val arr = s.replaceAll("[\\(\\)]", "").split(",").map(_.trim)
//  if (arr.length == 2)
//    (arr(0).toDouble, arr(1).toDouble)
//  else
//    None
// }

// business_rows.take(2).foreach(println)

// def getBusiness(lines: RDD[String]): RDD[Business] =
//  lines.map(line =>
//    {
//      val arr = line.split(",").map(_.trim)
//      Business(b_type = arr(1),
//        status = arr(3),
//        start_date = LocalDate.parse(arr(4), DateTimeFormatter.ofPattern("MM/dd/yyyy")),
//        end_date = LocalDate.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy")),
//        address = if (arr(8) == "" && arr(9) == "") None else Some(arr(8) + "," + arr(9)),
//        city = if (arr(12) == "") None else Some(arr(12)),
//        state = if (arr(13) == "") None else Some(arr(13)),
//        zip = if (arr(14) == "") None else Some(arr(14)),
//        // location = if(arr(arr.length - 1) == "") None else Some((0.0,0.0))//Some(stringtoTuple(arr(26)))
//        )
//    })

// // val business_data = getBusiness(business_rows).filter(row=>row.b_type == "Business")
// // business_data.take(5).foreach(println)
// // business_data.persist()




// val events_cols = events.first()
// val events_rows = events.filter(row => row != events_cols) //.map(line=>line.split(",").map(_.trim))

// def getEvents(lines: RDD[String]): RDD[Event] =
// 	lines.map(line =>
//  	{
//    	val arr = line.split(",").map(_.trim)
//    	Event(id = arr(0).toInt,
//    		e_type = arr(5),
//    		start_date = if (arr(2)(2) == '/') LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")) else LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm")),
//    		end_date = if (arr(3)(2) == '/') LocalDateTime.parse(arr(3), DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")) else LocalDateTime.parse(arr(2), DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm")),
//    		address = if (arr(7) == "") None else Some(arr(7)),
//    		borough = if (arr(6) == "") None else Some(arr(6))
//      )
//  	})

// val events_data = getEvents(events_rows)
// events_data.take(5).foreach(println)
// events_data.persist()