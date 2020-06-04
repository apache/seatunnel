package org.apache.spark.sql.execution.datasources.jdbc2

import org.apache.spark.sql.execution.datasources.jdbc2.JDBCOptions._
import org.apache.spark.sql.execution.datasources.jdbc2.JdbcUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}

class DefaultSource extends CreatableRelationProvider with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc2"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {

    val jdbcOptions = new JDBCOptions(parameters)
    val partitionColumn = jdbcOptions.partitionColumn
    val lowerBound = jdbcOptions.lowerBound
    val upperBound = jdbcOptions.upperBound
    val numPartitions = jdbcOptions.numPartitions

    val partitionInfo = if (partitionColumn.isEmpty) {
      assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not specified, " +
        s"'$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
      null
    } else {
      assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
        s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
          s"'$JDBC_NUM_PARTITIONS' are also required")
      JDBCPartitioningInfo(
        partitionColumn.get, lowerBound.get, upperBound.get, numPartitions.get)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    JDBCRelation(parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val options = new JDBCOptions(parameters)
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis

    var saveMode = mode match {
      case SaveMode.Overwrite => JDBCSaveMode.Overwrite
      case SaveMode.Append => JDBCSaveMode.Append
      case SaveMode.ErrorIfExists => JDBCSaveMode.ErrorIfExists
      case SaveMode.Ignore => JDBCSaveMode.Ignore
    }
    val parameterLower = parameters.map(kv => (kv._1.toLowerCase,kv._2))
    if(parameterLower.keySet.contains("savemode")){
      saveMode = if(parameterLower("savemode").equals(JDBCSaveMode.Update.toString)) JDBCSaveMode.Update else saveMode
    }

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        saveMode match {
          case JDBCSaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options, saveMode)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table)
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

    createRelation(sqlContext, parameters)
  }
}
