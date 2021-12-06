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
package io.github.interestinglab.waterdrop.utils

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import io.github.interestinglab.waterdrop.config.Common

object SparkClusterUtils {

  /**
   * TODO: 在cluster模式下是否还管用? 因为cluster模式下，driver已经运行在cluster上，无法再add local file,
   * TODO: 在cluster模式下，这个不好使！！！！！！,已经找不到对应的文件了!!!!
   * TODO: addfile vs --files
   * 否则还要通过--files指定.
   * */
  def addFiles(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.addFile(Paths.get(Common.appRootDir.toString, "config").toString, true)
    sparkSession.sparkContext.addFile(Paths.get(Common.appRootDir.toString, "lib").toString, true)
    sparkSession.sparkContext.addFile(Paths.get(Common.appRootDir.toString, "plugins").toString, true)
  }

  def addJarDependencies(sparkSession: SparkSession): Unit = {

    // TODO: spark-submit --jars 指定了jars,还有必要在这里addJar("")????
//    sparkSession.sparkContext.addJar("")
  }
}
