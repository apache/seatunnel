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
package io.github.interestinglab.waterdrop.apis

import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

trait BaseStructuredStreamingOutput extends ForeachWriter[Row] with BaseStructuredStreamingOutputIntra {

  /**
   * Things to do before process.
   * */
  def open(partitionId: Long, epochId: Long): Boolean

  /**
   * Things to do with each Row.
   * */
  def process(row: Row): Unit

  /**
   * Things to do after process.
   * */
  def close(errorOrNull: Throwable): Unit

  /**
   * Waterdrop Structured Streaming process.
   * */
  def process(df: Dataset[Row]): DataStreamWriter[Row]
}
