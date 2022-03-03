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
package org.apache.seatunnel.spark

import org.apache.seatunnel.shade.com.typesafe.config.Config

case class JdbcConfigs(config: Config) {

  import scala.collection.JavaConverters._
  import JdbcConfigs._
  lazy val driver: String = config.getString(DRIVER)
  lazy val url: String = config.getString(URL)
  lazy val user: String = config.getString(USER)
  lazy val password: String = config.getString(PASSWORD)
  lazy val preSqls: List[String] = {
    if (config.hasPath(PRE_SQLS)) {
      config.getStringList(PRE_SQLS).asScala.toList
    } else {
      List[String]()
    }
  }
  lazy val postSqls: List[String] = {
    if (config.hasPath(POST_SQLS)) {
      config.getStringList(POST_SQLS).asScala.toList
    } else {
      List[String]()
    }
  }
  lazy val saveMode: String = config.getString(SAVE_MODE)
  lazy val dbTable: String = config.getString(DB_TABLE)
  lazy val useSsl:String = config.getString(USE_SSL)

  // Custom mysql duplicate key update statement when saveMode is update
  lazy val customUpdateStmt: String = config.getString(CUSTOM_UPDATE_STMT)

  lazy val showSql: String = config.getString(SHOW_SQL)

  lazy val duplicateIncs: String = config.getString(DUPLICATE_INCS)
}

object JdbcConfigs{

  val DEFAULT_SQL_SEPARATOR = ";"

  val DRIVER: String = "driver"

  val URL: String = "url"

  val USER: String = "user"

  val PASSWORD: String = "password"

  val DB_TABLE: String = "dbTable"

  val PRE_SQLS: String = "preSqls"

  val POST_SQLS: String = "postSqls"

  val SAVE_MODE: String = "saveMode"

  val USE_SSL: String = "useSsl"

  val CUSTOM_UPDATE_STMT: String= "customUpdateStmt"

  val SHOW_SQL: String = "showSql"

  val DUPLICATE_INCS: String = "duplicateIncs"

}
