package datasetManagers.datasetCleaners

import java.time.LocalDateTime

import datasetStructure.{UberDatasetSchemaActual, UberDatasetSchemaCleaned}
import helper.DateTimeFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CleanUberDataset {
  def cleanDataset(uberDatasetFileName: String, spark: SparkSession, sc: SparkContext): (Array[String], RDD[(LocalDateTime, Float, Float)]) ={
    val dataset = sc.textFile(uberDatasetFileName)

    val uberDataset= dataset.map(line=> line.split(","))

    val csvHeader = uberDataset.first()

    val taxiDatasetRdd = uberDataset.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

//    println(taxiDatasetRdd.count())
    val relevantColumnsRdd = taxiDatasetRdd.map(line => (line(UberDatasetSchemaActual.dateTime._2), line(UberDatasetSchemaActual.lat._2),
      line(UberDatasetSchemaActual.long._2)))

//    println(relevantColumnsRdd.count())

    val dateTimeFormatter = new DateTimeFormatter("\"M/d/yyyy H:m:ss\"")
    val filteredDataset = relevantColumnsRdd.map(line => cleanData(line, dateTimeFormatter))
    val cleanDataset = filteredDataset.filter(line => line!= null)

    (UberDatasetSchemaCleaned.getHeaders, cleanDataset)
  }

  def cleanData(input: (String, String, String), dateTimeFormatter: DateTimeFormatter): (LocalDateTime, Float, Float) ={
    try {
      (dateTimeFormatter.formateDate(input._1.toString), input._2.toFloat, input._3.toFloat)
    } catch {
      case _: Exception => null
    }
  }
}
