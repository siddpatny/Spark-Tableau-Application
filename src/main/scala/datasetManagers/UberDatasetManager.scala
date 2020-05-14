package datasetManagers

import java.time.LocalDateTime

import datasetManagers.datasetCleaners.CleanUberDataset
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object UberDatasetManager {

  def profileDataset(taxiDatasetFileName: String, spark: SparkSession, sc: SparkContext): List[String] ={

    val dataset = sc.textFile(taxiDatasetFileName)

    val taxiDataset= dataset.map(line=> line.split(","))

    var profileList = List.empty[String]

    profileList = profileList :+ "Number of Rows in Original Uber Dataset = "+ taxiDataset.count().toString

    val csvHeader = taxiDataset.first()
    profileList :+= "Headers in Uber Dataset = "+ csvHeader.mkString(", ")

    val cleanRdd = cleanDataset(taxiDatasetFileName, spark, sc)

    profileList :+= "Number of Rows in Cleaned Uber Dataset = "+ (cleanRdd._2.count() + 1)
    profileList :+= "Headers in cleaned Uber Dataset = "+ cleanRdd._1.mkString(", ")

    val monthCount = cleanRdd._2.map(line => (line._1.getMonth, 1)).reduceByKey(_ + _).collect()
    monthCount.map(line => profileList :+= ("Number of Uber Pickups in " + line._1 + " = " + line._2))

    profileList
  }


  def cleanDataset(taxiDatasetFileName: String, spark: SparkSession, sc: SparkContext): (Array[String], RDD[(LocalDateTime, Float, Float)]) ={
    CleanUberDataset.cleanDataset(taxiDatasetFileName, spark, sc)
  }
}
