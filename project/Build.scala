import sbt._
import Keys._

object WaterDropBuild extends Build {

  lazy val root = Project(id="waterdrop",
    base=file(".")) aggregate(apis, core, doctor) dependsOn(core, doctor)

  lazy val apis = Project(id="waterdrop-apis",
    base=file("waterdrop-apis"))

  lazy val core = Project(id="waterdrop-core",
    base=file("waterdrop-core")) dependsOn(apis)

  lazy val doctor = Project(id="waterdrop-doctor",
    base=file("doctor"))
}
