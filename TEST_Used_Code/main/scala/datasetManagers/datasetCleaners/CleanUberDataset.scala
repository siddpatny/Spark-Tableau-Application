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


//    println(cleanDataset.count())

//    val cleanedPUTime = cleanDateTimeColumn(relevantColumnsRdd.map(line => line._1))
//    println(cleanedPUTime.count())
//    val latRdd = relevantColumnsRdd.filter(line => Try(line._2.toFloat).isSuccess)
//    val cleanedLatRdd = latRdd.map(line => line)
//    val longRdd = relevantColumnsRdd.filter(line => Try(line._3.toFloat).isSuccess)
//    println(latRdd.count())
//    println(longRdd.count())
//
//    (cleanedPUTime, latRdd, longRdd)
    (UberDatasetSchemaCleaned.getHeaders, cleanDataset)
  }

  def cleanDateTimeColumn(inputDateTimeRdd: RDD[(String)]) = {
    val dateTimeFormatter = new DateTimeFormatter("\"mm/dd/yyyy HH:mm:ss\"")
    val dateTimeRdd = inputDateTimeRdd.map(line => dateTimeFormatter.formateDate(line.toString))

    val cleanedDateTimeRdd = dateTimeRdd.filter(line => line!=null)

    cleanedDateTimeRdd
  }

}
