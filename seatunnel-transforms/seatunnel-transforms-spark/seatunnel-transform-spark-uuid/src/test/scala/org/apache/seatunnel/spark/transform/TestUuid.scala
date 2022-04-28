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
package org.apache.seatunnel.spark.transform

import java.security.SecureRandom

import junit.framework.TestCase.assertEquals
import org.apache.commons.math3.random.Well19937c
import org.junit.Test

class TestUuid {
  @Test
  def testUuid() {
    val uuid = new Uuid
    assertEquals(36, uuid.generate("").length)
    assertEquals(37, uuid.generate("u").length)
  }

  @Test
  def testSecureUuid() {
    val rand = new SecureRandom
    val seed = for (_ <- 0 until 728) yield rand.nextInt
    val prng = new Well19937c(seed.toArray)

    val uuid = new Uuid
    uuid.setPrng(prng)
    assertEquals(36, uuid.generate("").length)
    assertEquals(37, uuid.generate("u").length)
  }
}
