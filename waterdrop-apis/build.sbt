name         := "Waterdrop-apis"
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

// We forked and modified code of Typesafe config, the jar in unmanagedJars is packaged by InterestingLab
// Project: https://github.com/InterestingLab/config
unmanagedJars in Compile += file("lib/config-1.3.3-SNAPSHOT.jar")

libraryDependencies ++= Seq(
)

// TODO: exclude spark, hadoop by for all dependencies

dependencyOverrides += "com.google.guava" % "guava" % "15.0"
