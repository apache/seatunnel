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
  val packagePrefix: String = "org.apache.seatunnel." + engine
  val upperEngine: String = engine.substring(0, 1).toUpperCase() + engine.substring(1)
  val sourcePackage: String = packagePrefix + ".source"
  val transformPackage: String = packagePrefix + ".transform"
  val sinkPackage: String = packagePrefix + ".sink"
  val envPackage: String = packagePrefix + ".env"
  val baseSourceClass: String = packagePrefix + ".Base" + upperEngine + "Source"
  val baseTransformClass: String = packagePrefix + ".Base" + upperEngine + "Transform"
  val baseSinkClass: String = packagePrefix + ".Base" + upperEngine + "Sink"
}
