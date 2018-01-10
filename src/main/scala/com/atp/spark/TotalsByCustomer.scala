package com.atp.spark

import org.apache.spark._
import org.apache.log4j._

object TotalsByCustomer {
  def customerPurchasesKeyPairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Customer Totals")

    val input = sc.textFile("../SparkScala/customer-orders.csv")

    val purchasesHash = input.map(customerPurchasesKeyPairs)

    val purchasesTotals = purchasesHash.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false, numPartitions = 1)

    val results = purchasesTotals.collect

    results.foreach(println)


  }

}
