name := "SparkFun"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"

libraryDependencies += "net.debasishg" %% "redisclient" % "3.0"

//libraryDependencies += "com.databricks" %% "spark-redshift" % "0.6.0"