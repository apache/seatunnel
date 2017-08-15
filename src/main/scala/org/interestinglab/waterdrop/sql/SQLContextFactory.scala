package org.interestinglab.waterdrop.sql

import org.apache.spark.sql.SparkSession

/**
  * Singleton to get SQLContext instance.
  */
object SQLContextFactory {

    @transient  private var instance: SparkSession = _

    def getInstance(): SparkSession = {
        if (instance == null) {
            instance = SparkSession.builder().getOrCreate()
        }
        instance
    }
}
