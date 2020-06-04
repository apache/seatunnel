package org.apache.spark.sql.execution.datasources.jdbc2

import java.sql.{Connection, Driver, DriverPropertyInfo, SQLFeatureNotSupportedException}
import java.util.Properties

class DriverWrapper(val wrapped: Driver) extends Driver {
  override def acceptsURL(url: String): Boolean = wrapped.acceptsURL(url)

  override def jdbcCompliant(): Boolean = wrapped.jdbcCompliant()

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    wrapped.getPropertyInfo(url, info)
  }

  override def getMinorVersion: Int = wrapped.getMinorVersion

  def getParentLogger: java.util.logging.Logger = {
    throw new SQLFeatureNotSupportedException(
      s"${this.getClass.getName}.getParentLogger is not yet implemented.")
  }

  override def connect(url: String, info: Properties): Connection = wrapped.connect(url, info)

  override def getMajorVersion: Int = wrapped.getMajorVersion
}
