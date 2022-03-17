package org.apache.seatunnel.spark.sink.clickhouse.filetransfer

object RsyncFileTransfer extends FileTransfer {

  override def transferAndChown(sourcePath: String, targetPath: String): Unit = {
    throw new UnsupportedOperationException("not support rsync file transfer yet")
  }

  override def init(): Unit = {
    throw new UnsupportedOperationException("not support rsync file transfer yet")
  }

  override def transferAndChown(sourcePath: List[String], targetPath: String): Unit = {
    throw new UnsupportedOperationException("not support rsync file transfer yet")
  }

  override def close(): Unit = {
    throw new UnsupportedOperationException("not support rsync file transfer yet")
  }
}
