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
package io.github.interestinglab.waterdrop.serializer

import io.github.interestinglab.waterdrop.config.Config


// TODO : 是否需要checkConfig, prepare, 何时调用serializer的serialize, deserialize
abstract class BaseSerializer(config: Config) {

  val charset = if (config.hasPath("charset")) {
    config.getString("charset")
  } else {
    "utf-8"
  }

  /**
   * Deserialize array of bytes to String.
   */
  def deserialize(bytes: Array[Byte]): String

  /**
   * Serialize String to bytes of array.
   */
  def serialize(e: String): Array[Byte]
}
