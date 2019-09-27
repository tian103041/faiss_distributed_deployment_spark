package spark_redis_pubsub

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.JavaConversions._


object redis_connect extends Serializable{

  private def connectJedisCluster(hostAndPort:String): JedisCluster = {
    val nodes = new collection.mutable.HashSet[HostAndPort]
    hostAndPort.split(",").foreach{
      h =>
        val Array(host,port) = h.split(":")
        nodes += new HostAndPort(host,port.toInt)
    }
    val conf = new JedisPoolConfig
    conf.setMaxTotal(4)
    conf.setMaxIdle(4)
    new JedisCluster(nodes, 10000, 1000, conf)
  }

  // lazy 很重要，在rdd每个partition用，用lazy相当于在每个jvm建立一个jedisCluster
  lazy val jedisTestCluster1: JedisCluster = connectJedisCluster("10.193.22.154:11110")

}

