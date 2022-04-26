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
package org.apache.seatunnel.spark.hudi

/**
 * Configurations and defaults for Hudi source and sink
 */
object Config extends Serializable {

  /**
   * Hoodie base path
   */
  val HOODIE_BASE_PATH = "hoodie.base.path"

  /**
   * Hoodie table name
   */
  val HOODIE_TABLE_NAME = "hoodie.table.name"

  /**
   * Save mode
   */
  val SAVE_MODE = "save_mode"

  /**
   * Default save mode
   */
  val DEFAULT_SAVE_MODE = "append"

  /**
   * Hoodie data store read paths
   */
  val HOODIE_DATASTORE_READ_PATHS = "hoodie.datasource.read.paths"
}
