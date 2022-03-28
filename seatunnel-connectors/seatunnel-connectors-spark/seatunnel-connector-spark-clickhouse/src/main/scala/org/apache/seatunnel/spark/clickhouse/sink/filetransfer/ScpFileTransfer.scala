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

package org.apache.seatunnel.spark.clickhouse.sink.filetransfer

import org.apache.sshd.client.SshClient
import org.apache.sshd.client.session.ClientSession
import org.apache.sshd.scp.client.{ScpClient, ScpClientCreator}

class ScpFileTransfer(host: String) extends FileTransfer {

  var password: String = _

  def this(host: String, password: String) {
    this(host)
    this.password = password
  }

  private var scpClient: ScpClient = _
  private var session: ClientSession = _
  private var client: SshClient = _

  override def transferAndChown(sourcePath: String, targetPath: String): Unit = {

    // TODO override Scp to support zero copy
    scpClient.upload(sourcePath, targetPath, ScpClient.Option.Recursive, ScpClient.Option
      .TargetIsDirectory, ScpClient.Option.PreserveAttributes)

    // remote exec command to change file owner. Only file owner equal with server's clickhouse user can
    // make ATTACH command work.
    try {
      session.executeRemoteCommand("ls -l " + targetPath.substring(0, targetPath.stripSuffix("/").lastIndexOf("/")) +
        "/ | tail -n 1 | awk '{print $3}' | xargs -t -i chown -R {}:{} " + targetPath)
    } catch {
      case e: Exception =>
      // always return error cause xargs return shell command result
    }
  }

  override def transferAndChown(sourcePath: List[String], targetPath: String): Unit = {
    sourcePath.foreach(s => {
      transferAndChown(s, targetPath)
    })
  }

  override def init(): Unit = {
    client = SshClient.setUpDefaultClient()
    client.start()
    session = client.connect("root", this.host, 22).verify().getSession
    if (password != null) {
      session.addPasswordIdentity(this.password)
    }
    val isSuccess = session.auth.verify.isSuccess
    if (!isSuccess) {
      throw new IllegalArgumentException(s"ssh host '$host' verify failed, please check your config")
    }

    scpClient = ScpClientCreator.instance.createScpClient(session)

  }

  override def close(): Unit = {
    if (session != null && session.isOpen) {
      session.close()
    }
    if (client != null && client.isOpen) {
      client.stop()
      client.close()
    }
  }
}
