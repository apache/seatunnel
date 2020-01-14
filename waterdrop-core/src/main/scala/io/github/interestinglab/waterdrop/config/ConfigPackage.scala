package io.github.interestinglab.waterdrop.config

class ConfigPackage(engine: String) {
  val packagePrefix = "io.github.interestinglab.waterdrop." + engine
  val upperEnginx = engine.substring(0,1).toUpperCase() + engine.substring(1)
  val sourcePackage = packagePrefix + ".source"
  val transformPackage = packagePrefix + ".transform"
  val sinkPackage = packagePrefix + ".sink"
  val envPackage = packagePrefix + ".env"
  val baseSourcePackage = packagePrefix + ".Base" + upperEnginx + "Source"
  val baseTransformPackage = packagePrefix + ".Base" + upperEnginx + "Transform"
  val baseSinkPackage = packagePrefix + ".Base" + upperEnginx + "Sink"

}
