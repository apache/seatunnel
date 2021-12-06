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
package io.github.interestinglab.waterdrop.input.sparkstreaming.redis

import io.github.interestinglab.waterdrop.config.Config

case class RedisInfo(config: Config) {
  var host: String = config.getString("host")
  var password: String = config.getString("password")
  var prefKey: String = config.getString("prefKey")
  var queue: String = config.getString("queue")
  var maxTotal: Int = config.getInt("maxTotal")
  var maxIdle: Int = config.getInt("maxIdle")
  var maxWaitMillis: Long = config.getLong("maxWaitMillis")
  var connectionTimeout: Int = config.getInt("connectionTimeout")
  var soTimeout: Int = config.getInt("soTimeout")
  var maxAttempts: Int = config.getInt("maxAttempts")
}
