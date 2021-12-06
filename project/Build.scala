/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
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
