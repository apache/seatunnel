package org.apache.phoenix.spark

import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast

object CsUtil {

  def applyCs(c1: Option[Broadcast[SerializableWritable[Credentials]]]) {
    @transient val ugi = UserGroupInformation.getCurrentUser
    if (null != ugi && null != c1 && c1.isDefined && null != c1.get.value &&
      null != c1.get.value.value) {
      ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)
      ugi.addCredentials(c1.get.value.value)
    }
  }

}
