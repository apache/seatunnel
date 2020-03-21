package org.apache.phoenix.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.phoenix.spark.CsUtil._
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast

final class PhoenixRDD2(sc: SparkContext, table: String, columns: Seq[String], predicate: Option[String] = None, zkUrl: Option[String] = None, cre: Credentials,
                        dateAsTimestamp: Boolean = false, tenantId: Option[String] = None) extends PhoenixRDD(sc, table, columns, predicate, zkUrl,
  new Configuration, dateAsTimestamp, tenantId) {

  val c1: Option[Broadcast[SerializableWritable[Credentials]]] = Some(sc.broadcast(new SerializableWritable(cre)))

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[PhoenixRecordWritable] = {
    applyCs(c1)
    super.compute(split, context)
  }

}