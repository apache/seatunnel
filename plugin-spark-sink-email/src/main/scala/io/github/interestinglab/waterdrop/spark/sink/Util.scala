package io.github.interestinglab.waterdrop.spark.sink

import scala.util.{Success, Try}

object Utils {
  implicit class RichTry[T](t: Try[T]) {
    def toEither: Either[Throwable, T] = t.transform(s => Success(Right(s)), f => Success(Left(f))).get
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
                         optionalKeys: OptionalKeys[K] = OptionalKeys[K]()
                       ) {
    def unapply[V](m: Map[K, V]): Option[(requiredKeys.ResultType[V], optionalKeys.ResultType[V])] =
      for {
        req <- requiredKeys.unapplySeq(m)
        opt <- optionalKeys.unapplySeq(m)
      } yield (req, opt)
  }
}