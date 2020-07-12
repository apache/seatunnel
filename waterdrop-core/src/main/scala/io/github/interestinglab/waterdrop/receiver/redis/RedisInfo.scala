package io.github.interestinglab.waterdrop.receiver.redis

import com.typesafe.config.Config

case class RedisInfo(config: Config) {
  var host: String = config.getString("host")
  var password: String = config.getString("password")
  var prefKey: String = config.getString("prefKey")
  var queue: String = config.getString("queue")
  var maxTotal: Int = config.getInt("maxTotal")
  var maxIdle: Integer = config.getInt("maxIdle")
  var maxWaitMillis: Long = config.getLong("maxWaitMillis")
  var connectionTimeout: Int = config.getInt("connectionTimeout")
  var soTimeout: Int = config.getInt("soTimeout")
  var maxAttempts: Int = config.getInt("maxAttempts")
}
