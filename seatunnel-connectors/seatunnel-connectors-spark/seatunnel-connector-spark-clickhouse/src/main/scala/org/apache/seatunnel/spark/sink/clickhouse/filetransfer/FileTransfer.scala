package org.apache.seatunnel.spark.sink.clickhouse.filetransfer

abstract class FileTransfer {

  def init(): Unit

  def transferAndChown(sourcePath: String, targetPath: String): Unit

  def transferAndChown(sourcePath: List[String], targetPath: String): Unit

  def close(): Unit

}
