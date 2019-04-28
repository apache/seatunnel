name         := "Waterdrop"
version      := "1.3.2"
organization := "io.github.interestinglab.waterdrop"

scalaVersion := "2.11.8"

// resolved sbt assembly merging file conflicts.
assemblyMergeStrategy in assembly := {
  case PathList("org", "joda", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".thrift" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".fmpp" => MergeStrategy.first
  case "UnusedStubClass.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

dependencyOverrides += "com.google.guava" % "guava" % "15.0"

// The 'run', 'runMain' task uses all the libraries, including the ones marked with "provided".
// This is useful for running spark application in local
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))

// sbt native packager
enablePlugins(JavaAppPackaging)
enablePlugins(UniversalPlugin)

// only build and include fat jar in packaging
// the assembly settings
// we specify the name for our fat jar
//assemblyJarName in assembly := name + "-" + version + "-" + scalaVersion + ".jar"
assemblyJarName in assembly := name.value + "-" + version.value + "-" + scalaVersion.value + ".jar"

// removes all jar mappings in universal and appends the fat jar
mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, name) =>  ! name.endsWith(".jar")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

// the bash scripts classpath only needs the fat jar
scriptClasspath := Seq( (assemblyJarName in assembly).value )

import NativePackagerHelper.directory
mappings in Universal += file("README.md") -> "README.md"
mappings in Universal += file("LICENSE") -> "LICENSE"
mappings in Universal ++= directory("plugins")
mappings in Universal ++= directory("docs")
mappings in Universal ++= directory("config")
mappings in Universal ++= directory("bin")
