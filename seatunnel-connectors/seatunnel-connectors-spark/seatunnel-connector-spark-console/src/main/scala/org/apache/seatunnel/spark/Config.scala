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
package org.apache.seatunnel.spark

/**
 * Console configuration parameters and defaults
 */
object Config {

  /**
   * The nubmer of rows to show
   */
  val LIMIT = "limit"

  /**
   * The serializer (plain/json/schema)
   */
  val SERIALIZER = "serializer"

  /**
   * Default console show output
   */
  val PLAIN = "plain"

  /**
   * Convert dataframe to json and print
   */
  val JSON = "json"

  /**
   * Print the schema
   */
  val SCHEMA = "schema"

  /**
   * Default serializer
   */
  val DEFAULT_SERIALIZER = PLAIN

  /**
   * Default number of rows
   */
  val DEFAULT_LIMIT = 100

}
