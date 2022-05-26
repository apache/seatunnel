package org.apache.seatunnel.spark.transform.service.impl

import java.sql.Timestamp
import java.util

import org.apache.seatunnel.shade.com.typesafe.config.Config
import org.apache.seatunnel.spark.transform.EtlConfig.FIELDS
import org.apache.seatunnel.spark.transform.service.EtlHandler
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType, VarcharType}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap}

class DefaultHandler extends EtlHandler {

  override def handler(df: Dataset[Row], config: Config): Dataset[Row] = {
    var keys = config.getStringList(FIELDS)
    if (keys == null) keys = new util.ArrayList[String]()

    var fieldNameDefault = new HashMap[String, String]()
    for (i <- 0 until keys.size()) {
      val key = keys.get(i)
      if (key.contains("=")) {
        val firstIndex = key.indexOf("=")
        fieldNameDefault += (key.substring(0, firstIndex) -> key.substring(firstIndex + 1))
      } else fieldNameDefault += (key -> null)
    }

    df.mapPartitions(iter => {
      var result = ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row = iter.next()
        val fieldSeq = Buffer[Any]()
        for (i <- 0 until row.size) {
          val newField = if (row.isNullAt(i)) {
            val fieldName = row.schema.fields.apply(i).name
            val fieldType = row.schema.fields.apply(i).dataType

            val temp = fieldNameDefault.get(fieldName)
            if (temp.isDefined) {
              if (temp.get == null) getDefaultValueByDataType(fieldType) else transfromStringToRightType(temp.get, fieldType)
            } else {
              getDefaultValueByDataType(fieldType)
            }
          } else row.get(i)

          fieldSeq += newField
        }
        val newRow = Row.fromSeq(fieldSeq)
        result += newRow
      }
      result.iterator
    })(RowEncoder.apply(df.schema))

  }

  private def getDefaultValueByDataType(dataType: DataType): Any = {
    dataType match {
      case StringType => ""
      case ShortType => 0
      case IntegerType => 0
      case FloatType => 0f
      case DoubleType => 0d
      case LongType => 0L
      case BooleanType => false
      case DateType => new java.sql.Date(System.currentTimeMillis())
      case TimestampType => new Timestamp(System.currentTimeMillis())
      case _ => null
    }
  }

  private def transfromStringToRightType(value: String, dataType: DataType): Any = {
    dataType match {
      case StringType => value
      case ShortType => java.lang.Short.valueOf(value)
      case IntegerType => java.lang.Integer.valueOf(value)
      case FloatType => java.lang.Float.valueOf(value)
      case DoubleType => java.lang.Double.valueOf(value)
      case LongType => java.lang.Long.valueOf(value)
      case BooleanType => java.lang.Boolean.valueOf(value)
      case DateType => java.sql.Date.valueOf(value)
      case TimestampType => java.sql.Timestamp.valueOf(value)
      case _ => value
    }
  }

}

object DefaultHandler extends Serializable {
  def apply(): DefaultHandler = new DefaultHandler()
}
