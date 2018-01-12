package com.atp.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object PopularMoviesWithNames {
  /** Our main function where the action happens */

  def loadMovies(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("../ml-100k/u.item").getLines()

    for(line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")

    var dictNames = sc.broadcast(loadMovies())

    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )

    // Sort
    val sortedMovies = flipped.sortByKey()

    //LookUp name (titanic, 350)
    val namedMovies = sortedMovies.map(x => (dictNames.value(x._2),x._1 ))

    // Collect and print results
    val results = namedMovies.collect()

    results.foreach(println)
  }
}
