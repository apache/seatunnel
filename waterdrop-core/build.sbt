name         := "Waterdrop-core"
version      := "1.4.0"
organization := "io.github.interestinglab.waterdrop"

scalaVersion := "2.11.8"


val sparkVersion = "2.4.0"

// We should put all spark or hadoop dependencies here,
//   if coresponding jar file exists in jars directory of online Spark distribution,
//     such as spark-core-xxx.jar, spark-sql-xxx.jar
//   or jars in Hadoop distribution, such as hadoop-common-xxx.jar, hadoop-hdfs-xxx.jar
lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
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

// We forked and modified code of Typesafe config, the jar in unmanagedJars is packaged by InterestingLab
// Project: https://github.com/InterestingLab/config
unmanagedJars in Compile += file("lib/config-1.3.3-SNAPSHOT.jar")

libraryDependencies ++= Seq(

  // ------ Spark Dependencies ---------------------------------
  // spark distribution doesn't provide this dependency.
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
    exclude("org.spark-project.spark", "unused")
    exclude("net.jpountz.lz4", "unused"),
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  // --------------------------------------------------------

  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0",
  "org.apache.kudu" %% "kudu-spark2" % "1.7.0",
  "com.alibaba" % "QLExpress" % "3.2.0",
  "com.alibaba" % "fastjson" % "1.2.51",
  "com.alibaba" % "druid" % "1.1.10",
  "commons-lang" % "commons-lang" % "2.6",
  "io.thekraken" % "grok" % "0.1.5",
  "mysql" % "mysql-connector-java" % "5.1.6",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.6.1",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.apache.commons" % "commons-compress" % "1.15",
  "com.pingcap.tispark" % "tispark-core" % "1.1"
    excludeAll(ExclusionRule(organization="com.fasterxml.jackson.core")),
  "com.pingcap.tikv" % "tikv-client" % "1.1",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.39"
    excludeAll(ExclusionRule(organization="com.fasterxml.jackson.core")),
  "com.databricks" %% "spark-xml" % "0.5.0",
  "org.apache.httpcomponents" % "httpasyncclient" % "4.1.3"
).map(_.exclude("com.typesafe", "config"))

// TODO: exclude spark, hadoop by for all dependencies

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
