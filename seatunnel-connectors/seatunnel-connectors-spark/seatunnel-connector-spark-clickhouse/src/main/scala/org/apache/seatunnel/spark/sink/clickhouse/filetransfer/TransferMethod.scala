package org.apache.seatunnel.spark.sink.clickhouse.filetransfer

object TransferMethod extends Enumeration {
  type TransferMethod = Value
  val SCP, RSYNC = Value

  def getCopyMethod(method: String): TransferMethod = {
    val m = method.toLowerCase
    if ("scp".equals(m)) {
      SCP
    } else if ("rsync".equals(m)) {
      RSYNC
    } else {
      throw new IllegalArgumentException(s"not supported copy method '$method'")
    }
  }

}
