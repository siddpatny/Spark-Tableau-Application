name := "BigDataApplication_TaxisAndEventsNYC"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies ++= Seq("org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6")

libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"

//// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
//libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.1"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
