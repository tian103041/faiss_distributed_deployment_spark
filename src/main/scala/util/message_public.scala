package util

class message_public {

  def pub_master() : Unit = {

    val jedis_cluster = redis_connect.jedisTestCluster1
    jedis_cluster.publish("faiss_spark_to_master_data_pub","the faiss distributed data is ready!")
    jedis_cluster.close()
  }

}
