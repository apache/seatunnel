package io.github.interestinglab.waterdrop.output.utils

import com.mongodb.MongoClient
import com.typesafe.config.Config


class MongoClientUtil(crateMongoClient: () => MongoClient) extends Serializable{
  lazy val client = crateMongoClient()
  def getClient() : MongoClient = {
    client
  }
}

object MongoClientUtil {
  def apply(config: Config): MongoClientUtil = {
    val f = () =>{
      val client = new MongoClient(config.getString("host"), config.getInt("port"))
      sys.addShutdownHook {
        client.close()
      }
      client
    }
    new MongoClientUtil(f)
  }
}
