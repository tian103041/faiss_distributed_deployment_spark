package spark_redis_pubsub

object message_receive_from_user {

  def main(args: Array[String]): Unit = {

    val jedis_cluster = redis_connect.jedisTestCluster1

    new SubThread(jedis_cluster).run()

  }

}
