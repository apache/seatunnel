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
package org.apache.phoenix.spark

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast

object CsUtil {

  def applyCs(c1: Option[Broadcast[SerializableWritable[Credentials]]]): Unit = {
    @transient val ugi = UserGroupInformation.getCurrentUser
    if (null != ugi && null != c1 && c1.isDefined && null != c1.get.value &&
      null != c1.get.value.value) {
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)
      ugi.addCredentials(c1.get.value.value)
    }
  }

}
