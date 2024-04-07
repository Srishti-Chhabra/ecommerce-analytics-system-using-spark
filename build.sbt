name := "SparkProject"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.0.1")

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1" % "provided"