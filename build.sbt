name         := "Waterdrop"
version      := "0.1.0"
organization := "org.interestinglab.waterdrop"

scalaVersion := "2.11.8"
scalaBinaryVersion := "2.11"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion
    exclude("org.spark-project.spark", "unused"),
  "com.typesafe" % "config" % "1.3.1",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

resolvers += Resolver.mavenLocal

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")


// automatically check coding style before compile
scalastyleFailOnError := true
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value
