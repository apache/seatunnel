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
 * Configurations for ElasticSearch source and sink
 */
object Config extends Serializable {
  /**
   * Elastic search hosts configuration
   */
  val HOSTS = "hosts"

  /**
   * Elastic search index configuration
   */
  val INDEX = "index"

  /**
   * Elastic search index time format configuration
   */
  val INDEX_TIME_FORMAT = "index_time_format"

  /**
   * Elastic search index type configuration
   */
  val INDEX_TYPE = "index_type"

  /**
   * Elastic search default index
   */
  val DEFAULT_INDEX = "seatunnel"

  /**
   * Elastic search default index type
   */
  val DEFAULT_INDEX_TYPE = "_doc"

  /**
   * Elastic search default index time format
   */
  val DEFAULT_INDEX_TIME_FORMAT = "yyyy.MM.dd"
}
