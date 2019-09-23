package io.github.interestinglab.waterdrop.config

class ConfigPackage(engine: String) {
  val packagePrefix = "io.github.interestinglab.waterdrop." + engine
  val sourcePackage = packagePrefix + ".source"
  val transformPackage = packagePrefix + ".transform"
  val sinkPackage = packagePrefix + ".sink"
  val envPackage = packagePrefix + ".env"
}
