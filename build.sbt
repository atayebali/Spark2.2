name := "Spark2"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard  // Toss out META-INF files
  case x => MergeStrategy.first //If duplicates are found, pick first
}