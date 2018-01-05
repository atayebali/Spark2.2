package com.atp.spark

import org.apache.spark._
import org.apache.log4j._

object WordCountSorted {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Word Count")

    // Read each line of input data
    val input = sc.textFile("../SparkScala/book.txt")
    val words = input.flatMap(x => x.split("\\W+"))
    val wordCount = words.map(x => x.toLowerCase()).map(x => (x, 1)).reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey()
    for (res <- wordCount) {
      val count = res._1
      val word = res._2
      println(s"$word: $count")
    }
  }
}

