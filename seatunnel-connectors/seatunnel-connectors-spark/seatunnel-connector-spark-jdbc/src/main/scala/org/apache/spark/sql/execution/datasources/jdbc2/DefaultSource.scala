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
package org.apache.spark.sql.execution.datasources.jdbc2

import org.apache.spark.sql.execution.datasources.jdbc2.JdbcUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}

// SeaTunnel: DefaultSource for mysql to custom saveMode
class DefaultSource extends CreatableRelationProvider with RelationProvider
  with DataSourceRegister {

  override def shortName(): String = "jdbc2"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    val jdbcOptions = new JdbcOptionsInWrite(parameters)

    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)

    JDBCRelation(parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {

    val options = new JdbcOptionsInWrite(parameters)
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis

    var saveMode = mode match {
      case SaveMode.Overwrite => JDBCSaveMode.Overwrite
      case SaveMode.Append => JDBCSaveMode.Append
      case SaveMode.ErrorIfExists => JDBCSaveMode.ErrorIfExists
      case SaveMode.Ignore => JDBCSaveMode.Ignore
    }
    if (parameters.contains("saveMode")) {
      saveMode = if (parameters("saveMode").equals(JDBCSaveMode.Update.toString)) JDBCSaveMode.Update else saveMode
    }

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        saveMode match {
          case JDBCSaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url).contains(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options, saveMode)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, df, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options, saveMode)
            }

          case JDBCSaveMode.Update =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options, saveMode)

          case JDBCSaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options, saveMode)

          case JDBCSaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '${options.table}' already exists. SaveMode: ErrorIfExists.")

          case JDBCSaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, df, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options, saveMode)
      }
    } finally {
      conn.close()
    }

    // In fact, the instance of BaseRelation that is returned
    // by org.apache.spark.sql.execution.datasources.jdbc2.DefaultSource.createRelation
    // will not be used, so return empty to prevent some bug from createRelation.
    JDBCRelation(Array.empty, new JDBCOptions(parameters))(sqlContext.sparkSession)
  }
}
