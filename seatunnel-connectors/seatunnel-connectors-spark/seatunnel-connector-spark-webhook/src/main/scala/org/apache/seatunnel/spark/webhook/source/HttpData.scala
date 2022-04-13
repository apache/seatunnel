package org.apache.seatunnel.spark.webhook.source

import java.sql.Timestamp

/**
 * Streaming data read from local server will have this schema
 *
 * @param value - The payload POSTed to http endpoint.
 * @param timestamp - Timestamp of when it was put on a stream.
 */
case class HttpData(value: String, timestamp: Timestamp)
