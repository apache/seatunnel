package org.apache.spark.seatunnel.metrics.sink

import com.codahale.metrics.MetricRegistry
import org.apache.seatunnel.metrics.spark.SeatunnelMetricSink.SinkConfig
import org.apache.spark.internal.config
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.seatunnel.metrics.sink.SeatunnelMetricSink.SinkConfigProxy
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import java.util.Properties

object SeatunnelMetricSink {

  class SinkConfigProxy extends SinkConfig {
    // SparkEnv may become available only after metrics sink creation thus retrieving
    // SparkConf from spark env here and not during the creation/initialisation of PrometheusSink.
    @transient
    private lazy val sparkConfig = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf(true))

    // Don't use sparkConf.getOption("spark.metrics.namespace") as the underlying string won't be substituted.
    def metricsNamespace: Option[String] = sparkConfig.get(config.METRICS_NAMESPACE)

    def sparkAppId: Option[String] = sparkConfig.getOption("spark.app.id")

    def sparkAppName: Option[String] = sparkConfig.getOption("spark.app.name")

    def executorId: Option[String] = sparkConfig.getOption("spark.executor.id")
  }
}

class SeatunnelMetricSink(property: Properties,
                          registry: MetricRegistry,
                          sinkConfig: SinkConfig
                         )
  extends org.apache.seatunnel.metrics.spark.SeatunnelMetricSink(property, registry, sinkConfig) with Sink {

  // Constructor required by MetricsSystem::registerSinks() for spark >= 3.2
  def this(property: Properties, registry: MetricRegistry) = {
    this(
      property,
      registry,
      new SinkConfigProxy
    )
  }

  // Legacy Constructor required by MetricsSystem::registerSinks() for spark < 3.2
  def this(property: Properties, registry: MetricRegistry, securityMgr: SecurityManager) = {
    this(property, registry)
  }
}
