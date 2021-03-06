package com.atp.spark

import com.atp.spark.DegreesOfSeparation.hitCounter
import org.apache.spark._
import org.apache.spark.util.LongAccumulator
//import org.apache.spark.SparkContext.
import java.nio.charset.CodingErrorAction
import scala.util.control.Breaks._
import org.apache.log4j._

import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimBetter {

  var hitCounter:Option[LongAccumulator] = None
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, Array[String]] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, Array[String]] = Map()

    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> pluckRatings(fields.drop(1)))
      }
    }

    return movieNames
  }

  def pluckRatings(movieData: Array[String]): Array[String] = {
    var movieInfo: Array[String] = Array()
    movieInfo  = movieInfo :+ movieData(0)
    val genre = movieData.drop(4)

    for(i <- 0 until genre.length){
      if( genre(i).toInt > 0 ) { movieInfo = movieInfo :+ i.toString }
    }
    return movieInfo
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  def makePairs(userRatings:UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    return movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def computeJaccardSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    val total: Int = ratingPairs.size
    val m1 = ratingPairs.map(x => x._1).toArray
    val m2 = ratingPairs.map(x => x._2).toArray
    val intersect = m1.intersect(m2).distinct

    val union = m1.union(m2).distinct

    var score: Double = 0.0
    val den: Double = union.length

    if (den != 0) { score = intersect.length.toDouble/union.length.toDouble }
    return(score, total)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)


    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    hitCounter = Some(sc.longAccumulator("Hit Counter"))
    println("\nLoading movie names...")

    //Dict = (12 -> ('Hello", '10/10.2012', 'url:asdfas', '0', '0', '0', '0')

    val nameDict = loadMovieNames()
    val data = sc.textFile("../ml-100k/u.data")

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val raw_ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.

    //Only grab good movies
    val ratings = raw_ratings.filter(x => (x._2._2 >= 3.0))
    val joinedRatings = ratings.join(ratings)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".


    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0
    var movieID:Int = 50

    if (args.length > 0){ movieID = args(0).toInt }



    // Filter for movies with this sim that are "good" as defined by
    // our quality thresholds above

    val filteredResults = moviePairSimilarities.filter( x =>
    {
      val pair = x._1
      val sim = x._2
      (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
    }
    )

    // Sort by quality score.
    val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false)

    println("\nTop 10 similar movies for " + nameDict(movieID)(0))



      for (result <- results) {
        val (sim, pair) = (result._1, result._2)
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }


        val originalMovieInfo = nameDict(movieID)

        if ( compareGenre(originalMovieInfo, nameDict(similarMovieID)) & (hitCounter.get.value < 10 ) ) {
          println(nameDict(similarMovieID)(0) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
          if (hitCounter.isDefined) { hitCounter.get.add(1)}
        }
      }
  }
  def compareGenre(original: Array[String], newmovie: Array[String]): Boolean = {
    val originalGen = original.drop(1)
    val newGen = newmovie.drop(1)
//    println(s" original: ${originalGen.mkString(" ")}")
//    println(s" newGem: ${newGen.mkString(" " )}")

    return (originalGen.intersect(newGen).length >= 1)
  }
}


/*

b = Array(0, 1, 1)

for(i <- 0  until b.length){
  if (b(i).toInt > 0) {
    gen :+ i
  }
}

 */