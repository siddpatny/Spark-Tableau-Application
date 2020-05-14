package datasetManagers.datasetCleaners

import java.time.LocalDateTime

import datasetStructure.{NycTaxiDatasetSchemaActual, NycTaxiDatasetSchemaCleaned}
import helper.DateTimeFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CleanTaxiDataset {
  val nycTaxiDatasetSchemaActual = NycTaxiDatasetSchemaActual
  val nycTaxiDatasetSchemaCleaned = NycTaxiDatasetSchemaCleaned

  def cleanDataset(taxiDatasetFileName: String, spark: SparkSession, sc: SparkContext): (Array[String], RDD[(Int, LocalDateTime, LocalDateTime, Int, Int, Int)]) ={
    val dataset = sc.textFile(taxiDatasetFileName)

    val taxiDataset= dataset.map(line=> line.split(","))

    val csvHeader = taxiDataset.first()

    val taxiDatasetRdd = taxiDataset.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    println("Original Taxidataset" + taxiDatasetRdd.count())
    val relevantColumnsRdd = taxiDatasetRdd.filter(_.length > 3).map(line => (line(nycTaxiDatasetSchemaActual.vendorID._2), line(nycTaxiDatasetSchemaActual.pickupDateTime._2),
      line(nycTaxiDatasetSchemaActual.dropOffDateTime._2), line(nycTaxiDatasetSchemaActual.passenger_count._2),
      line(nycTaxiDatasetSchemaActual.PULocationID._2), line(nycTaxiDatasetSchemaActual.DOLocationID._2)))

    val dateTimeFormatter = new DateTimeFormatter("M/d/yyyy h:m:s a")
    val filteredDataset = relevantColumnsRdd.map(line => cleanData(line, dateTimeFormatter))
    val cleanDataset = filteredDataset.filter(line => line!= null)

    (NycTaxiDatasetSchemaCleaned.getHeaders, cleanDataset)
  }

  def cleanData(input: (String, String, String, String, String, String), dateTimeFormatter: DateTimeFormatter): (Int, LocalDateTime, LocalDateTime, Int, Int, Int) ={
    /** Filter Taxi data records, for a given set of years
     */
    try {
      try {
        val year1 = dateTimeFormatter.formateDate(input._2).getYear
        val year2 = dateTimeFormatter.formateDate(input._3).getYear
        if(year1 < 2015 || year1 > 2020 || year2 < 2015 || year2 > 2020 ){
          throw new Exception("Invalid year")
        }
        (input._1.toInt, dateTimeFormatter.formateDate(input._2), dateTimeFormatter.formateDate(input._3), input._4.toInt, input._5.toInt, input._6.toInt)
      } catch {
        case _: Exception => {
          val dateTimeFormatter1 = new DateTimeFormatter("yyyy-M-d HH:m:s")
          val year1 = dateTimeFormatter1.formateDate(input._2.toString).getYear
          val year2 = dateTimeFormatter1.formateDate(input._3.toString).getYear
          if(year1 < 2015 || year1 > 2020 || year2 < 2015 || year2 > 2020 ){
            throw new Exception("Invalid year")
          }
          (input._1.toInt, dateTimeFormatter1.formateDate(input._2), dateTimeFormatter1.formateDate(input._3), input._4.toInt, input._5.toInt, input._6.toInt)
        }
      }
    } catch {
      case _: Exception => null
    }
  }
}
