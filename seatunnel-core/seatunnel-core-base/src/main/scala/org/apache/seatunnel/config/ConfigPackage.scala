/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.config

class ConfigPackage(engine: String) {
  val packagePrefix = "org.apache.seatunnel." + engine
  val upperEngine = engine.substring(0, 1).toUpperCase() + engine.substring(1)
  val sourcePackage = packagePrefix + ".source"
  val transformPackage = packagePrefix + ".transform"
  val sinkPackage = packagePrefix + ".sink"
  val envPackage = packagePrefix + ".env"
  val baseSourceClass = packagePrefix + ".Base" + upperEngine + "Source"
  val baseTransformClass = packagePrefix + ".Base" + upperEngine + "Transform"
  val baseSinkClass = packagePrefix + ".Base" + upperEngine + "Sink"
}
