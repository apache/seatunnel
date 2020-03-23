package org.apache.phoenix.spark

import java.util.concurrent.TimeUnit

import org.I0Itec.zkclient.ZkClient

object ZkConnectUtil {

  def checkZkConnect(zkUrl: String): Unit = {
    var cli: ZkClient = null
    try {
      cli = new ZkClient(zkUrl, TimeUnit.SECONDS.toMillis(30).toInt,
        TimeUnit.SECONDS.toMillis(30).toInt)
    } finally cli.close()
  }

}
