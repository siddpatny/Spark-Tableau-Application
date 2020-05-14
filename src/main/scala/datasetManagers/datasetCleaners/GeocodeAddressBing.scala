package datasetManagers.datasetCleaners

import datasetStructure.AddressMappingSchema
import helper.Utils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scalaj.http.Http

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

object GeocodeAddressBing {
  /** Geocode the address string using Bing API
   */
  def geocodeMap(sc : SparkContext, key: String): RDD[String] = {
    val addressFileRdd = sc.textFile("datasets/cleanedEBDataset/csv_format/events/*.csv")
    val header = addressFileRdd.first()
    val addressRdd = addressFileRdd.filter(row => row != header).map(line => (line.split(",")(1),line.split(",")(2)))
      .distinct()

    println(addressRdd.count())
    var i = 0
    val bingResponse = addressRdd.map(line => {
      i+=1
      println(i + "," + line._1 + line._2)
      (line._1, line._2,
        extractAddressValue(line._1, line._2, Http("http://dev.virtualearth.net/REST/v1/Locations?key=" +
          key).param("q", line._2 + "," + line._1 + ",USA").asString.body, key))
    })
      .filter(line => line._3 != null).persist(StorageLevel.MEMORY_AND_DISK)

    println(bingResponse.count())

    Utils.reformatRddToSave(AddressMappingSchema.getHeaders, bingResponse, sc)
  }

  /** Parse every response from the Bing API
   */
  def extractAddressValue(origBorough: String, origAddress: String, response : String,key:String): String ={
    try {

      val jsonParsed = JSON.parseFull(response)
      val resourceSets = jsonParsed.get.asInstanceOf[HashMap.HashTrieMap[String, ::[Map[String, ::[HashMap.HashTrieMap[String, Any]]]]]].get("resourceSets")

      val resources:HashMap.HashTrieMap[String, Any] = resourceSets.get.head.get("resources")  match {
        case None => throw new Exception("No return value")
        case Some(x) => x.head
      }

      val coordinates: ::[Double] = resources.get("point") match {
        case None => throw new Exception("No return value")
        case Some(x) => x.asInstanceOf[Map.Map2[String, ::[Double]]].get("coordinates").get
      }

      val lat = coordinates.head

      val long = coordinates.tail.head

      //Todo : Need to check estimated count first
      val addressMap = resources.get("address").get.asInstanceOf[Map[String, Any]]

      val zipcode:String = addressMap.get("postalCode")  match {
        case None => extractAddressValue(origBorough,origAddress,Http("http://dev.virtualearth" +
          ".net/REST/v1/Locations/"+lat+","+long+"?key=" + key).asString.body,key)
        case Some(x) => x.toString
      }

//      val formatted_address = try {"\"" + addressMap.get("formattedAddress").get.toString + "\"" } catch {case _: Exception => null}

      val locality = addressMap.get("locality")  match {
        case None => origBorough
        case Some(x) => x.toString
      }

      zipcode
//
//      if(origBorough.compareToIgnoreCase(locality) == 0)
//        zipcode
//      else
//        {
//          println(locality+"::"+origBorough)
//          null
//        }
    }
    catch {
      case e :Exception => println("inside " + e); null
    }
  }
}
