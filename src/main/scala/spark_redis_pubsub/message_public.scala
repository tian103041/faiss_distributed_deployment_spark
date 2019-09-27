package spark_redis_pubsub

import redis.clients.jedis.Jedis

class message_public {

  def pub_master() : Unit = {

    val jedis = new Jedis("10.193.22.154", 11110)

    //"faiss_spark_to_master_data_pub"
    jedis.publish("faiss_spark_to_master_data_pub","the faiss distributed data is ready!")

    jedis.close()
  }

}
