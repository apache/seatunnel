package org.apache.seatunnel.spark.jdbc.source.util

import org.apache.spark.sql.jdbc.JdbcDialect

class HiveDialet extends JdbcDialect{
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:hive2")
  }

  override def quoteIdentifier(colName: String): String = {
    if(colName.contains(".")){
      val colName1 = colName.substring(colName.indexOf(".")+1)
      return s"`$colName1`"
    }
    s"`$colName`"
  }
}
