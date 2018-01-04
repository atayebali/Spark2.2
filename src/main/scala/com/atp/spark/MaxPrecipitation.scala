package com.atp.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaxPrecipitation {
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val precipitation = fields(3).toInt
    (stationID, entryType, precipitation)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPrecip")

    // Read each line of input data
    val lines = sc.textFile("../SparkScala/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but TMAX entries
    val maxPrecipation = parsedLines.filter(x => x._2 == "PRCP")

    // Convert to (stationID, temperature)
    val stationPrecipation = maxPrecipation.map(x => (x._1, x._3.toInt))

    // Reduce by stationID retaining the minimum temperature found
    val maxPrecipByStation = stationPrecipation.reduceByKey( (x,y) => max(x,y))

    // Collect, format, and print the results
    val results = maxPrecipByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = s"$temp %"
      println(s"$station max Preciption: $formattedTemp")
    }

  }
}
