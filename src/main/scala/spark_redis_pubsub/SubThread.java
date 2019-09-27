package spark_redis_pubsub;

import redis.clients.jedis.JedisCluster;

public class SubThread extends Thread {

    private final JedisCluster jediscluster;
    private final Subscriber subscriber = new Subscriber();
    private final String channel = "faiss_spark_to_master_data_pub";

    public SubThread(JedisCluster jediscluster) {
        super("SubThread");
        this.jediscluster = jediscluster;
    }

    @Override
    public void run() {
        System.out.println(String.format("subscribe redis, channel %s, thread will be blocked", channel));
        try {
            jediscluster.subscribe(subscriber, channel);
        } catch (Exception e) {
            System.out.println(String.format("subsrcibe channel error, %s", e));
        }
        }
}

