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
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "commons-lang" % "commons-lang" % "2.6",
  "io.thekraken" % "grok" % "0.1.5",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.3"
)

resolvers += Resolver.mavenLocal

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")


// automatically check coding style before compile
scalastyleFailOnError := true
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// antlr4 source code generatioin is invoked in command: sbt compile
antlr4Settings
antlr4Version in Antlr4 := "4.5.3"
antlr4PackageName in Antlr4 := Some("org.interestinglab.waterdrop.configparser")
antlr4GenListener in Antlr4 := false
antlr4GenVisitor in Antlr4 := true

// resolved sbt assembly merging file conflicts.
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "SimpleLog.class" => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "UnusedStubClass.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}