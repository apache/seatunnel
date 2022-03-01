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
package org.apache.seatunnel.spark.sink

import scala.language.higherKinds
import scala.util.{Success, Try}

object Utils {
  implicit class RichTry[T](t: Try[T]) {
    def toEither: Either[Throwable, T] =
      t.transform(s => Success(Right(s)), f => Success(Left(f))).get
  }

  case class MapIncluding[K](keys: Seq[K], optionally: Seq[K] = Seq()) {
    def unapply[V](m: Map[K, V]): Option[(Seq[V], Seq[Option[V]])] =
      if (keys.forall(m.contains)) {
        Some((keys.map(m), optionally.map(m.get)))
      } else {
        None
      }
  }
  sealed trait MapRequirements[K] {
    type ResultType[V]
    def unapplySeq[V](m: Map[K, V]): Option[ResultType[V]]
  }
  case class RequiredKeys[K](keys: K*) extends MapRequirements[K] {
    type ResultType[V] = Seq[V]
    def unapplySeq[V](m: Map[K, V]): Option[Seq[V]] =
      if (keys.forall(m.contains)) {
        Some(keys.map(m))
      } else {
        None
      }
  }
  case class OptionalKeys[K](keys: K*) extends MapRequirements[K] {
    type ResultType[V] = Seq[Option[V]]
    def unapplySeq[V](m: Map[K, V]): Option[Seq[Option[V]]] = Some(keys.map(m.get))
  }
  case class MapWith[K](
      requiredKeys: RequiredKeys[K] = RequiredKeys[K](),
      optionalKeys: OptionalKeys[K] = OptionalKeys[K]()) {
    def unapply[V](m: Map[K, V]): Option[(requiredKeys.ResultType[V], optionalKeys.ResultType[V])] =
      for {
        req <- requiredKeys.unapplySeq(m)
        opt <- optionalKeys.unapplySeq(m)
      } yield (req, opt)
  }
}
