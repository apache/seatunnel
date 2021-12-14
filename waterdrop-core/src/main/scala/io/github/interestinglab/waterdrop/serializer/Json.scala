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

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

object Json {

  // TODO : implement checkConfig
  /**
   *  Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  def checkConfig(): (Boolean, String) = (true, "")



  /**
   * Serialize Map[String, JValue] to Json String
   */
  def serialize(e: Map[String, AnyRef]): String = {

    JSON.toJSONString(e, SerializerFeature.WriteMapNullValue)
  }
}
