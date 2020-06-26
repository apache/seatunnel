import sbt._
import Keys._

object WaterDropBuild extends Build {

  lazy val root = Project(id="waterdrop",
    base=file(".")) aggregate(apis, core, doctor, config) dependsOn(core, doctor)

  lazy val config = Project(id="waterdrop-config",
    base=file("waterdrop-config"))

  lazy val apis = Project(id="waterdrop-apis",
    base=file("waterdrop-apis")) dependsOn(config)

  lazy val core = Project(id="waterdrop-core",
    base=file("waterdrop-core")) dependsOn(apis, config)

  lazy val doctor = Project(id="waterdrop-doctor",
    base=file("doctor"))
}
