name         := "Waterdrop-core"
version      := "1.1.1"
organization := "io.github.interestinglab.waterdrop"

scalaVersion := "2.11.8"


val sparkVersion = "2.4.0"

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

// Change dependepcy scope to "provided" by : sbt -DprovidedDeps=true <task>
val providedDeps = Option(System.getProperty("providedDeps")).getOrElse("false")

providedDeps match {
  case "true" => {
    println("providedDeps = true")
    libraryDependencies ++= providedDependencies.map(_ % "provided")
  }
  case "false" => {
    println("providedDeps = false")
    libraryDependencies ++= providedDependencies.map(_ % "compile")
  }
}

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
    exclude("org.spark-project.spark", "unused")
    exclude("net.jpountz.lz4", "unused"),
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.spark" %% "spark-hive" % sparkVersion ,
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0",
  "org.apache.kudu" %% "kudu-spark2" % "1.7.0",
  "com.alibaba" % "QLExpress" % "3.2.0",
  "com.alibaba" % "fastjson" % "1.2.47",
  "commons-lang" % "commons-lang" % "2.6",
  "io.thekraken" % "grok" % "0.1.5",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.3",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.apache.commons" % "commons-compress" % "1.15",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.39" exclude("com.google.guava","guava") excludeAll(ExclusionRule(organization="com.fasterxml.jackson.core"))
)

// For binary compatible conflicts, sbt provides dependency overrides.
// They are configured with the dependencyOverrides setting.
dependencyOverrides += "com.google.guava" % "guava" % "15.0"

resolvers += Resolver.mavenLocal

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")


// automatically check coding style before compile
scalastyleFailOnError := true
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// antlr4 source code generatioin is invoked in command: sbt compile
antlr4Settings
antlr4Version in Antlr4 := "4.5.3"
antlr4PackageName in Antlr4 := Some("io.github.interestinglab.waterdrop.configparser")
antlr4GenListener in Antlr4 := false
antlr4GenVisitor in Antlr4 := true

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)