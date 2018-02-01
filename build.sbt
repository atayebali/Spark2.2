name := "Spark2"

version := "0.1"

val sparkVersion = "2.2.0"
val sparkPackage = "org.apache.spark"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  sparkPackage %% "spark-core" % sparkVersion,
  sparkPackage %% "spark-sql" % sparkVersion //For spark 2.2.0 I need to include this for sparkSession to work.
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard  // Toss out META-INF files
  case x => MergeStrategy.first //If duplicates are found, pick first
}