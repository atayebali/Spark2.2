//package com.atp.spark
//
//
//
//
//import org.apache.spark.SparkContext._
//import org.apache.spark.sql._
//import sqlContext.implicits._
//import org.apache.log4j._
//
//
//object LVHotels {
//  def main(args: Array[String]) {
//
//    // Set the log level to only print errors
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    //Create Session
//    val spark = new SparkContext("local[*]", "Hotels in LV" )
//
//    //Spin up a DF
//    val df = spark.read.format("csv")
//      .option("header", "true")
//      .option("delimiter", ";")
//      .load("../SparkScala/lv.csv")
//
//    //Schema dumped for debugging
//    //df.printSchema()
//
//    //Display Hotel Name and Counts of the traveler types
//    df.select("Hotel name", "Traveler type").groupBy("Hotel name", "Traveler type").count
//        .sort(desc("Hotel name"))
//      .coalesce(1)
//      .write.format("csv")
//      .option("header", "true")
//      .save("data")
//
//  }
//}