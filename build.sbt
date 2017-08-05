name         := "StreamingETL"
version      := "0.1.0"
organization := "org.interestinglab.waterdrop"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
    exclude("org.spark-project.spark", "unused"),
  "com.typesafe" % "config" % "1.3.1",
  "org.json4s" %% "json4s-jackson" % "3.4.2"
)

resolvers += Resolver.mavenLocal

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
